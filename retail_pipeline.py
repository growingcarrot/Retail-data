import os
import io
import logging
import sqlite3
import pandas as pd
import argparse
from datetime import datetime, timedelta
import sys
import hashlib
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Initialize environment variables
load_dotenv()

# Configuration
CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = "76byj86oc9kf"
DB_FILE = "retail_data.db"  # SQLite database file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("ingestion.log"), logging.StreamHandler()]
)


def compute_file_hash(data_bytes):
    return hashlib.sha256(data_bytes).hexdigest()


def log_table_exist(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_ingestion_log (
            file_name TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL,
            ingestion_time DATETIME NOT NULL
        )
    """)
    conn.commit()


def has_file_changed(file_name, new_hash, conn):
    log_table_exist(conn)
    cur = conn.cursor()
    cur.execute("SELECT file_hash FROM file_ingestion_log WHERE file_name = ?", (file_name,))  # noqa: E501
    row = cur.fetchone()
    if row is None:
        return True
    return row[0] != new_hash


def record_file_hash(file_name, file_hash, conn):
    log_table_exist(conn)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO file_ingestion_log (file_name, file_hash, ingestion_time)
        VALUES (?, ?, ?)
        ON CONFLICT(file_name) DO UPDATE SET
            file_hash = excluded.file_hash,
            ingestion_time = excluded.ingestion_time
    """, (file_name, file_hash, datetime.now()))
    conn.commit()


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        id INTEGER PRIMARY KEY,
        name TEXT,
        job TEXT,
        email TEXT,
        account_id TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stores (
        id INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        opening TEXT,  -- SQLite doesn't have TIME type
        closing TEXT,
        type TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        ean INTEGER,
        brand TEXT,
        description TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER PRIMARY KEY,
        client_id INTEGER,
        product_id INTEGER,
        store_id INTEGER,
        transaction_time TEXT,
        quantity INTEGER,
        account_id TEXT,
        process_date TEXT,
        processed_at TEXT DEFAULT (DATETIME('now'))
    )
    """)

    log_table_exist(conn)
    conn.commit()
    logging.info("Database tables created/verified")


def date_check_in_transactions(conn, process_date):
    """Check if transactions exist for a given date"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM transactions WHERE DATE(process_date) = ? LIMIT 1",
        (process_date,)
    )
    return cursor.fetchone() is not None


def delete_transactions(conn, process_date):
    """Delete existing transactions for a given date"""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM transactions WHERE DATE(process_date) = ?",
        (process_date,)
    )
    deleted_count = cursor.rowcount
    conn.commit()
    logging.info(f"Deleted {deleted_count} existing transactions for {process_date}")  # noqa: E501
    return deleted_count


def process_static_files(blob_client, conn):
    """Process and load static files into database"""
    static_files = {
        "clients.csv": "clients",
        "products.csv": "products",
        "stores.csv": "stores"
    }

    for blob_name, table_name in static_files.items():
        try:
            blob_data = blob_client.get_blob_client(blob_name).download_blob().readall()  # noqa: E501
            file_hash = compute_file_hash(blob_data)

            if not has_file_changed(blob_name, file_hash, conn):
                logging.info(f"Skipped {blob_name} (no change detected)")
                continue

            df = pd.read_csv(io.BytesIO(blob_data), sep=';')

            # Transformations
            if blob_name == "stores.csv":
                # Split coordinates
                df['latlng'] = df['latlng'].str.strip('()')
                df[["latitude", "longitude"]] = df["latlng"].str.split(",", expand=True).astype(float)  # noqa: E501
                df = df.drop(columns=["latlng"])

            # Load into SQLite
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            record_file_hash(blob_name, file_hash, conn)
            logging.info(f"Loaded {len(df)} records into {table_name}")

        except Exception as e:
            logging.error(f"Error processing {blob_name}: {str(e)}")


def process_transactions(blob_client, conn, transaction_date):
    """Process transaction files for a specific date"""
    # Check if data exists for the processing date
    if date_check_in_transactions(conn, transaction_date):
        logging.info(f"Existing transactions found for {transaction_date} - deleting before reprocessing")  # noqa: E501
        delete_transactions(conn, transaction_date)

    # Get account_id mapping
    account_map = pd.read_sql("SELECT id, account_id FROM clients", conn)
    account_dict = account_map.set_index('id')['account_id'].to_dict()

    all_transactions = []
    for hour in range(8, 21):
        blob_name = f"transactions_{transaction_date}_{hour}.csv"
        try:
            blob_client_instance = blob_client.get_blob_client(blob_name)
            if blob_client_instance.exists():
                # Download and read CSV
                blob_data = blob_client_instance.download_blob().readall()

                # Read the first line to check if it's a valid header
                raw_text = blob_data.decode("utf-8")
                first_line = raw_text.splitlines()[0]
                if first_line.startswith("#") or "this file contains" in first_line.lower():  # noqa: E501
                    logging.error(f"Invalid header: first row appears to be a comment -> '{first_line}'")  # noqa: E501
                    raise ValueError(f"{blob_name} has an invalid header")

                # If valid, continue to read CSV normally
                df = pd.read_csv(io.BytesIO(blob_data), sep=';')

                # Add account_id
                df["account_id"] = df["client_id"].map(account_dict)

                # Create timestamp
                df["transaction_time"] = pd.to_datetime(
                    df["date"] + " " +
                    df["hour"].astype(str) + ":" +
                    df["minute"].astype(str)
                ).dt.strftime("%Y-%m-%d %H:%M:%S")

                all_transactions.append(df)
                logging.info(f"Processed {blob_name}: {len(df)} transactions")
        except Exception as e:
            logging.error(f"Error processing {blob_name}: {str(e)}")

    if all_transactions:
        final_df = pd.concat(all_transactions)
        final_df["process_date"] = transaction_date
        final_df = final_df[[
            "transaction_id", "client_id", "product_id", "store_id",
            "transaction_time", "quantity", "account_id", "process_date"
        ]]

        # Load into database
        final_df.to_sql("transactions", conn, if_exists="append", index=False)
        logging.info(f"Loaded {len(final_df)} transactions for {transaction_date}")  # noqa: E501
    else:
        logging.warning(f"No transactions found for {transaction_date}")


def run_pipeline(process_date):
    # Set up Azure connection
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client(CONTAINER_NAME)

    # Set up SQLite database
    conn = sqlite3.connect(DB_FILE)

    try:
        # Database initialization
        create_tables(conn)

        # Process static files
        process_static_files(blob_client, conn)

        # Process transactions
        transaction_date = process_date.strftime("%Y-%m-%d")
        process_transactions(blob_client, conn, transaction_date)

        logging.info("Ingestion completed successfully")

    except Exception as e:
        logging.critical(f"Pipeline failed: {str(e)}", exc_info=True)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run data pipeline with either a specific date or auto mode.")  # noqa: E501
    parser.add_argument("--date", type=str, help="Process date (YYYY-MM-DD)")
    parser.add_argument("--auto", action="store_true", help="Auto mode (process yesterday)")  # noqa: E501

    # Show help if no arguments are passed
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    if args.date:
        process_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    elif args.auto:
        process_date = datetime.today().date() - timedelta(days=1)
    else:
        parser.print_help()
        sys.exit(1)

    run_pipeline(process_date)


if __name__ == "__main__":
    main()
