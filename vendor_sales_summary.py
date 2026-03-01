import pandas as pd
import logging
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text

# =====================================================
# 1. SETUP LOGGING WITH TIMESTAMPED FILE
# =====================================================
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f"logs/get_vendor_summary_{timestamp}.log"

logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

logging.info(f"Log file created: {log_filename}")

# =====================================================
# 2. DATABASE CONNECTION
# =====================================================
DATABASE_URL = "postgresql+psycopg2://postgres:akasmik@localhost:5432/Inventory_vendor"
engine = create_engine(DATABASE_URL)


# =====================================================
# 3. CREATE MAIN TABLE IF NOT EXISTS
# =====================================================
def create_vendor_summary_table():
    try:
        logging.info("Creating vendor_sales_summary table if not exists...")

        create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS vendor_sales_summary (
            "VendorNumber" BIGINT,
            "VendorName" VARCHAR(100),
            "Brand" BIGINT,
            "Description" VARCHAR(200),
            "PurchasePrice" DECIMAL(15,2),
            "ActualPrice" DECIMAL(15,2),
            "Volume" DECIMAL(15,2),
            "TotalPurchaseQuantity" DECIMAL(15,2),
            "TotalPurchaseDollars" DECIMAL(15,2),
            "TotalSalesQuantity" DECIMAL(15,2),
            "TotalSalesDollars" DECIMAL(15,2),
            "TotalSalesPrice" DECIMAL(15,2),
            "TotalExciseTax" DECIMAL(15,2),
            "FreightCost" DECIMAL(15,2),
            "GrossProfit" DECIMAL(15,2),
            "ProfitMargin" DECIMAL(15,2),
            "StockTurnover" DECIMAL(15,2),
            "SalesToPurchaseRatio" DECIMAL(15,2),
            PRIMARY KEY ("VendorNumber", "Brand")
        );
        """)

        with engine.begin() as conn:
            conn.execute(create_table_sql)

        logging.info("Table creation check complete.")

    except Exception as e:
        logging.error("Error creating vendor_sales_summary table.")
        logging.error(traceback.format_exc())
        raise e


# =====================================================
# 4. RUN SQL SUMMARY QUERY
# =====================================================
def create_vendor_summary():
    try:
        logging.info("Running SQL summary query...")

        query = """
        WITH FreightSummary AS (
            SELECT 
                "VendorNumber",
                SUM(("Freight")::numeric) AS "FreightCost"
            FROM vendor_invoice
            GROUP BY "VendorNumber"
        ),

        PurchaseSummary AS (
            SELECT
                p."VendorNumber",
                p."VendorName",
                p."Brand",
                p."Description",
                p."PurchasePrice",
                pp."Price" AS "ActualPrice",
                pp."Volume",
                SUM((p."Quantity")::numeric) AS "TotalPurchaseQuantity",
                SUM((p."Dollars")::numeric) AS "TotalPurchaseDollars"
            FROM purchases p
            JOIN purchase_prices pp
                ON p."Brand"::text = pp."Brand"::text
            WHERE p."PurchasePrice" > 0
            GROUP BY 
                p."VendorNumber", p."VendorName", p."Brand",
                p."Description", p."PurchasePrice", pp."Price", pp."Volume"
        ),

        SalesSummary AS (
            SELECT
                "VendorNo"::bigint AS "VendorNumber",
                "Brand",
                SUM(("SalesQuantity")::numeric) AS "TotalSalesQuantity",
                SUM(("SalesDollars")::numeric) AS "TotalSalesDollars",
                SUM(("SalesPrice")::numeric) AS "TotalSalesPrice",
                SUM(("ExciseTax")::numeric) AS "TotalExciseTax"
            FROM sales
            GROUP BY "VendorNo", "Brand"
        )

        SELECT
            ps."VendorNumber",
            ps."VendorName",
            ps."Brand",
            ps."Description",
            ps."PurchasePrice",
            ps."ActualPrice",
            ps."Volume",
            ps."TotalPurchaseQuantity",
            ps."TotalPurchaseDollars",
            ss."TotalSalesQuantity",
            ss."TotalSalesDollars",
            ss."TotalSalesPrice",
            ss."TotalExciseTax",
            fs."FreightCost"
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps."VendorNumber" = ss."VendorNumber"
            AND ps."Brand"::text = ss."Brand"::text
        LEFT JOIN FreightSummary fs
            ON ps."VendorNumber" = fs."VendorNumber"
        ORDER BY ps."TotalPurchaseDollars" DESC;
        """

        df = pd.read_sql_query(query, engine)
        logging.info("SQL summary query executed successfully.")
        return df

    except Exception as e:
        logging.error("Error executing vendor summary SQL.")
        logging.error(traceback.format_exc())
        raise e


# =====================================================
# 5. CLEAN DATA
# =====================================================
def clean_data(df):
    try:
        logging.info("Cleaning data...")

        df["Volume"] = df["Volume"].astype(float)
        df.fillna(0, inplace=True)

        df["VendorName"] = df["VendorName"].astype(str).str.strip()
        df["Description"] = df["Description"].astype(str).str.strip()

        df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
        df["ProfitMargin"] = (df["GrossProfit"] / df["TotalSalesDollars"].replace(0, 1)) * 100
        df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"].replace(0, 1)
        df["SalesToPurchaseRatio"] = df["TotalSalesDollars"] / df["TotalPurchaseDollars"].replace(0, 1)

        logging.info("Data cleaning completed.")
        return df

    except Exception as e:
        logging.error("Error cleaning data.")
        logging.error(traceback.format_exc())
        raise e


# =====================================================
# 6. SAFE UPSERT INTO POSTGRESQL
# =====================================================
def insert_into_database(df):
    """
    Inserts DataFrame safely using a staging table + UPSERT.
    Avoids huge multi-row inserts and avoids PK conflicts.
    """
    try:
        logging.info("Starting safe upsert from staging table...")

        staging_table = "vendor_sales_summary_stage"

        # 1. Write staging table (replace)
        logging.info(f"Writing staging table '{staging_table}'...")
        df.to_sql(
            staging_table,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500
        )

        logging.info("Staging table written. Executing UPSERT...")

        cols = [
            "VendorNumber", "VendorName", "Brand", "Description", "PurchasePrice", "ActualPrice", "Volume",
            "TotalPurchaseQuantity", "TotalPurchaseDollars", "TotalSalesQuantity", "TotalSalesDollars",
            "TotalSalesPrice", "TotalExciseTax", "FreightCost", "GrossProfit", "ProfitMargin",
            "StockTurnover", "SalesToPurchaseRatio"
        ]

        quoted_cols = ", ".join(f'"{c}"' for c in cols)

        update_assignments = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in ("VendorNumber", "Brand")
        )

        upsert_sql = f"""
        INSERT INTO vendor_sales_summary ({quoted_cols})
        SELECT {quoted_cols}
        FROM {staging_table}
        ON CONFLICT ("VendorNumber", "Brand") DO UPDATE
        SET {update_assignments};
        
        DROP TABLE IF EXISTS {staging_table};
        """

        with engine.begin() as conn:
            conn.execute(text(upsert_sql))

        logging.info("UPSERT completed successfully.")

    except Exception as e:
        logging.error("Error during database UPSERT.")
        logging.error(traceback.format_exc())

        # cleanup staging table
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
        except:
            logging.warning("Failed to drop staging table during cleanup.")

        raise e


# =====================================================
# 7. MAIN PIPELINE
# =====================================================
def main():
    try:
        logging.info("Pipeline started...")

        create_vendor_summary_table()
        df = create_vendor_summary()
        df = clean_data(df)
        insert_into_database(df)

        logging.info("Pipeline completed successfully.")

    except Exception as e:
        logging.critical("Pipeline failed!")
        logging.critical(traceback.format_exc())
        print("Pipeline failed. Check log file for details:", log_filename)


# =====================================================
# 8. RUN SCRIPT
# =====================================================
if __name__ == "__main__":
    main()
