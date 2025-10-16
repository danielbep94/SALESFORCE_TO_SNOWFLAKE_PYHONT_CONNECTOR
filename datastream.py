import os
import time
import pathlib
import gzip
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector as sf
from datetime import datetime
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# 0) ENV Setup
# ─────────────────────────────────────────────────────────────
# Load environment variables from the specified production file.
# This keeps sensitive credentials and configuration separate from the code.
load_dotenv(dotenv_path=".envprod")

# ─────────────────────────────────────────────────────────────
# 1) SALESFORCE AUTH
# ─────────────────────────────────────────────────────────────
def salesforce_auth(client: httpx.Client):
    """
    Authenticates with Salesforce using the OAuth 2.0 client_credentials flow.

    Args:
        client: The httpx.Client instance for efficient HTTP requests.

    Returns:
        A tuple containing (access_token: str, instance_url: str).
    """
    # Retrieve necessary credentials from environment variables
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    
    # Salesforce OAuth endpoint for the specific sandbox/environment.
    # NOTE: Moving this URL to .envprod would make it easier to switch between environments (e.g., UAT/PROD).

    auth_data = {
        "grant_type": "client_credentials",
        "client_id": consumer_key,
        "client_secret": consumer_secret,
    }

    print("🔑 Authenticating with Salesforce…")
    
    # Post credentials to get the access token
    resp = client.post(auth_url, data=auth_data)
    
    # Ensure the request was successful (raises exception for 4xx/5xx errors)
    resp.raise_for_status() 
    
    auth = resp.json()
    access_token = auth["access_token"]
    instance_url = auth["instance_url"]
    
    print("✅ Auth OK")
    print(f"🌐 Instance URL: {instance_url}")
    return access_token, instance_url

# ─────────────────────────────────────────────────────────────
# 2) CONFIG (Global Parameters & Schemas)
# ─────────────────────────────────────────────────────────────
API_VER = "v61.0"

# Create a unique output folder name based on the current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_DIR = f"./sf_export_{timestamp}"

# Bulk API Parameters
MAX_RECORDS = 100000  # Max records to fetch per pagination batch from the Bulk API
CHUNK_ROWS = 250_000 # Chunk size used by pandas for memory-efficient CSV-to-Parquet conversion

# Salesforce Object Query Language (SOQL)
# Fetches all required fields for the Account object, ordered by modification date.
SOQL = """
SELECT Id, DAN_ERPAddressRegisteredDate__c,
DAN_ERPAddressSyncStatus__c,
DAN_IdERPAddress__c,
DAN_ERPRegisteredDate__c, DAN_ERPSyncStatus__c, DAN_ERPSyncResult__c , SystemModstamp
FROM Account
ORDER BY SystemModstamp
"""

# --- Data Type Mappings (Crucial for Type Safety) ---

# 1. Columns to explicitly parse as datetime/timestamp in Pandas (from CSV string)
PANDAS_DATETIME_COLS = [
    "DAN_ERPRegisteredDate__c", 
    "SystemModstamp",
    "DAN_ERPAddressRegisteredDate__c" 
]

# 2. Columns to explicitly read as string/object to prevent issues with Nulls/type inference
PANDAS_STRING_COLS = [
    "Id", 
    "DAN_ERPSyncStatus__c", 
    "DAN_ERPSyncResult__c",
    "DAN_ERPAddressSyncStatus__c", 
    "DAN_IdERPAddress__c"          
]

# 3. Explicit PyArrow Schema for the Parquet output and Snowflake DDL validation
# Use 'us' (microseconds) precision and 'UTC' timezone for all timestamps.
ACCOUNT_SCHEMA = pa.schema([
    pa.field("Id", pa.string()),
    pa.field("DAN_ERPAddressRegisteredDate__c", pa.timestamp('us', tz='UTC')),
    pa.field("DAN_ERPAddressSyncStatus__c", pa.string()),
    pa.field("DAN_IdERPAddress__c", pa.string()),                               
    pa.field("DAN_ERPRegisteredDate__c", pa.timestamp('us', tz='UTC')),
    pa.field("DAN_ERPSyncStatus__c", pa.string()),
    pa.field("DAN_ERPSyncResult__c", pa.string()),
    pa.field("SystemModstamp", pa.timestamp('us', tz='UTC')),
])


# ─────────────────────────────────────────────────────────────
# 3) HELPERS (SALESFORCE EXTRACT & PARQUET TRANSFORM)
# ─────────────────────────────────────────────────────────────
GZIP_MAGIC = b"\x1f\x8b" # Standard magic bytes for Gzip files

def get_headers(access_token, content_type="application/json"):
    """Generates standard request headers including Authorization."""
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    if content_type:
        headers["Content-Type"] = content_type
    
    # Request CSV data and ask for Gzip compression for faster transfer
    if content_type == "text/csv":
        headers["Accept"] = "text/csv"
        headers["Accept-Encoding"] = "gzip"
        
    return headers

def ensure_dirs():
    """Creates the timestamped local output directories for CSV and Parquet files."""
    csv_dir = pathlib.Path(OUT_DIR, "csv")
    pq_dir = pathlib.Path(OUT_DIR, "parquet")
    csv_dir.mkdir(parents=True, exist_ok=True)
    pq_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir, pq_dir

def is_gzip_file(path: str) -> bool:
    """Checks the file's binary signature to determine if it is Gzip compressed."""
    try:
        with open(path, "rb") as f:
            return f.read(2) == GZIP_MAGIC
    except IOError:
        return False

def safe_rename(src: pathlib.Path, dst: pathlib.Path):
    """
    Renames a file. If the target exists (due to concurrency/timing), 
    a timestamp suffix is appended to the new filename to avoid failure.
    """
    try:
        src.rename(dst)
    except FileExistsError:
        ts = datetime.now().strftime("%H%M%S%f")
        dst = dst.with_name(f"{dst.stem}_{ts}{dst.suffix}")
        src.rename(dst)
    return dst

def create_bulk_job(client: httpx.Client, base_url: str, soql: str, access_token: str) -> str:
    """Submits the SOQL query to the Salesforce Bulk API and returns the job ID."""
    headers = get_headers(access_token)
    r = client.post(
        f"{base_url}/jobs/query",
        headers=headers,
        # Requesting data in CSV format
        json={"operation": "query", "query": soql, "contentType": "CSV"},
        timeout=60,
    )
    r.raise_for_status()
    job = r.json()
    print(f"🆔 Created Bulk job: {job['id']}")
    return job["id"]

def wait_for_completion(client: httpx.Client, base_url: str, job_id: str, access_token: str):
    """Polls the job status every 3 seconds until it's 'JobComplete'."""
    headers = get_headers(access_token)
    print("⏳ Waiting for job to complete…")
    while True:
        r = client.get(f"{base_url}/jobs/query/{job_id}", headers=headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        state = j["state"]
        if state in ("JobComplete", "Failed", "Aborted"):
            if state != "JobComplete":
                raise RuntimeError(f"❌ Job failed: {j}")
            print(f"✅ Job complete ({j.get('numberRecordsProcessed', 0)} records).")
            return
        time.sleep(3)

def stream_results(client: httpx.Client, base_url: str, job_id: str, csv_dir: pathlib.Path, access_token: str) -> list[str]:
    """
    Downloads result chunks using the Sforce-Locator for pagination and saves them locally.
    Handles Gzip encoding negotiation.
    """
    print("⬇️ Downloading job results…")
    part_paths = []
    locator = None
    part = 1
    headers = get_headers(access_token, content_type="text/csv")

    while True:
        params = {"maxRecords": str(MAX_RECORDS)}
        if locator:
            params["locator"] = locator # Use the locator for the next page of results

        # Use streaming to handle potentially very large data responses
        with client.stream(
            "GET",
            f"{base_url}/jobs/query/{job_id}/results",
            headers=headers,
            params=params,
            timeout=None, # Allow indefinite time for large file transfer
        ) as r:
            r.raise_for_status()
            
            # Extract the locator for the *next* batch
            locator = r.headers.get("Sforce-Locator")
            enc = (r.headers.get("Content-Encoding") or "").lower().strip()
            ext = ".csv.gz" if enc == "gzip" else ".csv"
            out_path = csv_dir / f"part-{part:05d}{ext}"

            # Write raw chunk bytes to the file
            with open(out_path, "wb") as f:
                for chunk in r.iter_bytes():
                    f.write(chunk)

            # Check content against the expected extension for robustness
            is_file_gzip = is_gzip_file(out_path)
            if is_file_gzip and not out_path.name.endswith(".gz"):
                # File is Gzip, but doesn't have .gz extension. Fix it.
                fixed = out_path.with_suffix(out_path.suffix + ".gz")
                out_path = safe_rename(out_path, fixed)
            elif not is_file_gzip and out_path.name.endswith(".gz"):
                # File is not Gzip, but has .gz extension. Fix it.
                fixed = out_path.with_suffix("") 
                out_path = safe_rename(out_path, fixed)

            part_paths.append(str(out_path))
            print(f"📦 Downloaded {out_path} | next locator={locator}")
            part += 1

            # Exit when locator is missing or explicitly 'null'
            if not locator or locator.lower() == "null":
                break

    return part_paths

def empty_parquet_with_header(csv_path: str, pq_path: pathlib.Path):
    """
    Handles the edge case where the Salesforce job returns 0 records. 
    Creates an empty Parquet file with the correct schema, preventing downstream failures.
    """
    print(f"⚠️ Job returned 0 records. Creating empty Parquet file: {pq_path}")
    
    # Get column names based on the target schema
    cols = [field.name for field in ACCOUNT_SCHEMA]
    
    # Create arrays with zero length, matching the schema's types
    arrays = [pa.array([], type=ACCOUNT_SCHEMA.field(c).type) for c in cols]
    empty_tbl = pa.Table.from_arrays(arrays, schema=ACCOUNT_SCHEMA)
    
    # Write the empty but correctly structured table to Parquet
    pq.write_table(empty_tbl, pq_path, compression="snappy")


def csv_to_parquet_chunked(csv_path: str, pq_dir: pathlib.Path) -> str:
    """
    Transforms the local CSV file into an optimized Parquet file using chunking 
    and explicit type coercion to match the target PyArrow schema.
    """
    p = pathlib.Path(csv_path)
    base = p.name.replace(".csv.gz", "").replace(".csv", "")
    pq_path = pq_dir / f"{base}.parquet"

    compression = "gzip" if is_gzip_file(csv_path) else None
    first_chunk = True
    writer = None
    total_rows = 0

    try:
        # Define dtypes for string columns to prevent accidental numerical casting (e.g., on NaN/empty strings)
        string_dtypes = {col: str for col in PANDAS_STRING_COLS}

        # Read the CSV file in memory-efficient chunks
        for chunk in pd.read_csv(
            csv_path, 
            compression=compression, 
            low_memory=False, 
            chunksize=CHUNK_ROWS,
            parse_dates=False,    # Disable pandas' guess-work on dates
            dtype=string_dtypes,  # Apply string types upfront
        ):
            # --- EXPLICIT DATA TYPE CONVERSION (The core transformation step) ---
            for col in PANDAS_DATETIME_COLS:
                # Convert the column to datetime, forcing errors to NaT (Null) and setting to UTC
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce', utc=True)

            # Convert the Pandas DataFrame chunk to a PyArrow Table
            # This step validates the data against the strict ACCOUNT_SCHEMA.
            table = pa.Table.from_pandas(
                chunk, 
                schema=ACCOUNT_SCHEMA, 
                preserve_index=False
            )
            # ------------------------------------------------------------------
            
            if first_chunk:
                # Initialize the ParquetWriter only on the first chunk
                writer = pq.ParquetWriter(pq_path, schema=table.schema, compression="snappy")
                first_chunk = False
            
            writer.write_table(table)
            total_rows += len(chunk)

        if writer is None:
            # If the loop never ran (empty file), call the empty handler
            empty_parquet_with_header(csv_path, pq_path)
        else:
            # Close the writer to finalize the Parquet file metadata
            writer.close()

        print(f"🧱 Wrote {pq_path} ({total_rows:,} rows)")
        return str(pq_path)
    finally:
        # Ensure writer is closed even if an exception occurred during writing
        if writer is not None and not getattr(writer, 'is_closed', lambda: False)():
            try:
                writer.close()
            except Exception:
                pass

def delete_job(client: httpx.Client, base_url: str, job_id: str, access_token: str):
    """Deletes the Salesforce Bulk Query job to clean up server-side resources."""
    try:
        headers = get_headers(access_token)
        client.delete(f"{base_url}/jobs/query/{job_id}", headers=headers, timeout=30)
        print(f"🗑️ Deleted job {job_id}")
    except Exception as e:
        # Log cleanup failure but don't halt the pipeline
        print(f"⚠️ Could not delete job {job_id}: {e}")

# ─────────────────────────────────────────────────────────────
# 4) SNOWFLAKE (CONNECT & LOAD)
# ─────────────────────────────────────────────────────────────
# Connection parameters are retrieved from .envprod
SNOWFLAKE_CONN_INFO = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR")
)

# Snowflake object names (retrieved from env, with defaults)
STAGE_NAME = os.getenv("SF_STAGE_NAME", "ACCOUNT_STAGE")
FILE_FORMAT_NM = os.getenv("SF_FILE_FORMAT", "ACCOUNT_PARQUET_FF")
TARGET_TABLE = os.getenv("SF_TARGET_TABLE", "ACCOUNT_SALESFORCE")

def sf_connect():
    """Establishes and returns a connection to Snowflake."""
    # Assuming the authenticator is configured correctly (e.g., 'externalbrowser' for SSO)
    print("🔗 Connecting to Snowflake…")
    return sf.connect(**SNOWFLAKE_CONN_INFO)

def sf_ensure_objects(cur):
    """
    Sets the Snowflake context and ensures the stage, file format, and 
    target table exist with the required schema.
    """
    # Set context for execution safety
    cur.execute(f"USE WAREHOUSE {SNOWFLAKE_CONN_INFO['warehouse']}")
    cur.execute(f"USE DATABASE {SNOWFLAKE_CONN_INFO['database']}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_CONN_INFO['schema']}")

    # Create the Parquet file format and the internal stage
    cur.execute(f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT_NM} TYPE=PARQUET;")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME};")

    # Drop and Recreate the table. This guarantees a clean load with the latest schema.
    # NOTE: For incremental loads, this should be replaced with MERGE or TRUNCATE/INSERT.
    print(f"⚠️ Dropping and recreating table {TARGET_TABLE} to ensure new schema is applied.")
    cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE};")

    # Target table DDL (Data Definition Language) - Columns MUST match the Parquet schema
    # Use TIMESTAMP_NTZ as the source Parquet data is already in UTC.
    cur.execute(f"""
        CREATE TABLE {TARGET_TABLE} (
            ID STRING,
            DAN_ERPADDRESSREGISTEREDDATE__C TIMESTAMP_NTZ,
            DAN_ERPADDRESSSYNCSTATUS__C STRING,
            DAN_IDERPADDRESS__C STRING,
            DAN_ERPREGISTEREDDATE__C TIMESTAMP_NTZ,
            DAN_ERPSYNCSTATUS__C STRING,
            DAN_ERPSYNCRESULT__C STRING,
            SYSTEMMODSTAMP TIMESTAMP_NTZ
        );
    """)
    print(f"✅ Table created with new schema: {TARGET_TABLE}")

def sf_put_and_copy_all(cur, parquet_dir: pathlib.Path):
    """Uploads local Parquet files to the Snowflake stage and copies them into the target table."""
    files = sorted(parquet_dir.glob("part-*.parquet"))
    if not files:
        raise SystemExit(f"❌ No Parquet files found in {parquet_dir.resolve()}")
    print(f"📦 Found {len(files)} Parquet file(s) to load.")

    for idx, f in enumerate(files, 1):
        file_name = f.name
        # Resolve the absolute path and convert to POSIX format required by Snowflake PUT
        local_path = f.resolve().as_posix()
        print(f"\n[{idx}/{len(files)}] PUT {file_name} → @{STAGE_NAME}")
        
        # 1. PUT file (Upload local file to Snowflake stage)
        cur.execute(f"PUT 'file://{local_path}' @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        print(f"✅ PUT done for {file_name}")

        # 2. COPY file (Load data from stage into the table)
        print(f"    COPY INTO {TARGET_TABLE} from {file_name}")
        cur.execute(f"""
            COPY INTO {TARGET_TABLE}
            FROM @{STAGE_NAME}/{file_name}
            FILE_FORMAT=(FORMAT_NAME={FILE_FORMAT_NM})
            -- Ensures columns are mapped by name, ignoring case
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            ON_ERROR='ABORT_STATEMENT';
        """)
        print(f"✅ COPY done for {file_name}")
        
        # 3. CLEANUP 
        try:
            # Remove the file from the stage immediately after a successful load
            cur.execute(f"REMOVE @{STAGE_NAME}/{file_name}")
            print(f"✅ REMOVE done for {file_name} from stage.")
        except Exception as e:
            print(f"⚠️ Could not remove file {file_name} from stage: {e}")

# ─────────────────────────────────────────────────────────────
# 5) MAIN Execution Logic
# ─────────────────────────────────────────────────────────────
def main():
    """Orchestrates the entire Danone DataStream pipeline."""
    
    print("🚀 Starting Danone DataStream (Salesforce → Parquet → Snowflake)")
    
    # --- SALESFORCE EXTRACT & TRANSFORM PHASE ---
    # Use a persistent httpx Client for all SF API calls
    with httpx.Client(timeout=60.0) as client:
        # 1. Authentication and Setup
        access_token, instance_url = salesforce_auth(client)
        base_url = f"{instance_url}/services/data/{API_VER}"
        csv_dir, pq_dir = ensure_dirs()
        print(f"📁 Export directory: {OUT_DIR}")

        # 2. SF Bulk API Orchestration
        job_id = create_bulk_job(client, base_url, SOQL, access_token)
        wait_for_completion(client, base_url, job_id, access_token)
        parts = stream_results(client, base_url, job_id, csv_dir, access_token)

        # 3. Transformation (CSV to Parquet)
        print("\n📂 Converting CSV → Parquet (chunked, explicitly typed)…")
        parquet_files = [csv_to_parquet_chunked(p, pq_dir) for p in parts]
        print(f"🎯 Done. Parquet files created: {len(parquet_files)}")

        # 4. Cleanup Salesforce Job
        delete_job(client, base_url, job_id, access_token)
        print("✅ Salesforce process complete.")

    # --- SNOWFLAKE LOAD PHASE ---
    conn = None
    cur = None
    try:
        # 5. Connect and Prepare Target
        conn = sf_connect()
        cur = conn.cursor()
        sf_ensure_objects(cur) 

        # 6. Upload and Load Data
        sf_put_and_copy_all(cur, pathlib.Path(pq_dir))
        conn.commit()
        print("\n🎯 All files loaded successfully into:", TARGET_TABLE)
    except Exception as e:
        print(f"🚨 FATAL ERROR during Snowflake load. Rolling back: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # 7. Close Connection gracefully
        try:
            if cur: cur.close()
            if conn: conn.close()
            print("🔒 Snowflake connection closed.")
        except Exception:
            pass # Ignore errors during closing

if __name__ == "__main__":
    main()