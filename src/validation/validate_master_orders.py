from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

MASTER_ORDERS_PATH = (
    PROJECT_ROOT
    / "processed"
    / "master_orders"
)


# ---------------------------------------------------------
# Validate directory
# ---------------------------------------------------------

print("\n" + "=" * 65)
print("OLIST MASTER DATA QUALITY CHECK")
print("=" * 65)


if not MASTER_ORDERS_PATH.exists():
    raise FileNotFoundError(
        f"master_orders not found: {MASTER_ORDERS_PATH}"
    )


success_file = MASTER_ORDERS_PATH / "_SUCCESS"

if not success_file.exists():
    raise RuntimeError(
        "_SUCCESS file not found in master_orders."
    )


parquet_files = list(
    MASTER_ORDERS_PATH.glob("*.parquet")
)

if not parquet_files:
    raise RuntimeError(
        "No Parquet files found in master_orders."
    )


print(
    f"Parquet files found: {len(parquet_files)}"
)


# ---------------------------------------------------------
# Start Spark
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Olist-Data-Quality")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:

    print("\n[1] Reading master_orders...")

    df = spark.read.parquet(
        str(MASTER_ORDERS_PATH)
    )


    # -----------------------------------------------------
    # Required columns
    # -----------------------------------------------------

    print("\n[2] Checking required columns...")

    required_columns = {
        "order_id",
        "order_status"
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:

        raise RuntimeError(
            "Missing required columns: "
            + ", ".join(sorted(missing_columns))
        )


    print("Required columns found.")


    # -----------------------------------------------------
    # Row count
    # -----------------------------------------------------

    print("\n[3] Checking row count...")

    row_count = df.count()

    print(
        f"Total rows: {row_count}"
    )

    if row_count == 0:

        raise RuntimeError(
            "master_orders contains zero rows."
        )


    # -----------------------------------------------------
    # Null order IDs
    # -----------------------------------------------------

    print("\n[4] Checking null order IDs...")

    null_order_ids = (
        df
        .filter(
            col("order_id").isNull()
        )
        .count()
    )

    print(
        f"Null order IDs: {null_order_ids}"
    )

    if null_order_ids > 0:

        raise RuntimeError(
            f"Found {null_order_ids} null order IDs."
        )


    # -----------------------------------------------------
    # Duplicate information
    # -----------------------------------------------------

    print("\n[5] Checking duplicate order IDs...")

    duplicate_order_ids = (

        df
        .groupBy("order_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()

    )

    print(
        "Order IDs appearing multiple times:",
        duplicate_order_ids
    )

    # We do NOT fail here because master_orders may contain
    # multiple item-level records for one order.


    # -----------------------------------------------------
    # Final result
    # -----------------------------------------------------

    print("\n" + "=" * 65)
    print("DATA QUALITY CHECK PASSED")
    print("=" * 65)

    print(
        f"Rows              : {row_count}"
    )

    print(
        f"Null Order IDs    : {null_order_ids}"
    )

    print(
        f"Duplicate Order IDs: {duplicate_order_ids}"
    )

    print("=" * 65)


finally:

    spark.stop()
