import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging
import time

# --------------------------------------------------
# Logging Configuration
# --------------------------------------------------
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --------------------------------------------------
# Database Engine
# --------------------------------------------------
username = "postgres"
password = "akasmik"
host = "localhost"
port = "5432"
database = "Inventory_vendor"

engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)

# --------------------------------------------------
# Helper: Check if table exists in the database
# --------------------------------------------------
def table_exists(table_name):
    query = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = :name
        );
    """)
    with engine.connect() as conn:
        return conn.execute(query, {"name": table_name}).scalar()

# --------------------------------------------------
# Chunk-Based Ingestion (Only missing tables)
# --------------------------------------------------
def load_missing_tables():
    data_path = "data"
    chunksize = 50_000

    logging.info("=== Starting Selective Chunk-Based Ingestion ===")
    print("Starting ingestion for ONLY missing tables...\n")

    for file in os.listdir(data_path):

        if not file.endswith(".csv"):
            continue

        table_name = file.replace(".csv", "")
        full_path = os.path.join(data_path, file)

        # Skip existing tables
        if table_exists(table_name):
            print(f"⏩ Skipping {file} — table '{table_name}' already exists.")
            logging.info(f"Skipping {file}: table exists.")
            continue

        print(f"\nProcessing: {file}")
        logging.info(f"Processing {file}")

        start = time.time()

        try:
            # Drop table to avoid partial reloads
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

            first = True

            # IMPORTANT: Removed 'errors' argument (old pandas doesn't support it)
            for chunk in pd.read_csv(full_path, chunksize=chunksize, dtype=str, encoding="utf-8"):

                chunk.to_sql(
                    table_name,
                    engine,
                    if_exists="replace" if first else "append",
                    index=False,
                    method="multi"
                )

                first = False

            elapsed = (time.time() - start) / 60
            print(f"✔ Completed: {file} (Time: {elapsed:.2f} mins)")
            logging.info(f"Completed {file} → {table_name}")

        except Exception as e:
            print(f"❌ Error loading {file}: {e}")
            logging.error(f"ERROR loading {file}: {str(e)}")
            continue

    print("\n✔ Selective ingestion complete.")
    logging.info("=== Selective Ingestion Completed ===")
# --------------------------------------------------
# Run Script
# --------------------------------------------------
if __name__ == "__main__":
    load_missing_tables()
