import os

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofweek,
    datediff,
    when,
    sum,
    count,
    collect_set,
    broadcast,
    coalesce,
    lit
)

from src.utils.logger import get_logger


# ============================================================
# 1. LOAD CONFIGURATION
# ============================================================

load_dotenv("config.env")

RAW_PATH = os.getenv("RAW_PATH", "./raw")
PROCESSED_PATH = os.getenv("PROCESSED_PATH", "./processed")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "OlistPipeline")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

logger = get_logger("spark_pipeline")


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# 3. READ DATA
# ============================================================

def read_data(spark):

    logger.info("Reading Olist datasets...")

    orders = spark.read.csv(
        f"{RAW_PATH}/olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )

    order_items = spark.read.csv(
        f"{RAW_PATH}/olist_order_items_dataset.csv",
        header=True,
        inferSchema=True
    )

    customers = spark.read.csv(
        f"{RAW_PATH}/olist_customers_dataset.csv",
        header=True,
        inferSchema=True
    )

    products = spark.read.csv(
        f"{RAW_PATH}/olist_products_dataset.csv",
        header=True,
        inferSchema=True
    )

    payments = spark.read.csv(
        f"{RAW_PATH}/olist_order_payments_dataset.csv",
        header=True,
        inferSchema=True
    )

    category_translation = spark.read.csv(
        f"{RAW_PATH}/product_category_name_translation.csv",
        header=True,
        inferSchema=True
    )

    logger.info(f"Orders: {orders.count()}")
    logger.info(f"Order Items: {order_items.count()}")
    logger.info(f"Customers: {customers.count()}")
    logger.info(f"Products: {products.count()}")
    logger.info(f"Payments: {payments.count()}")

    return (
        orders,
        order_items,
        customers,
        products,
        payments,
        category_translation
    )


# ============================================================
# 4. CLEAN ORDERS
# ============================================================

def clean_orders(orders):

    logger.info("Cleaning orders...")

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for column_name in date_columns:

        orders = orders.withColumn(
            column_name,
            to_timestamp(column_name)
        )

    # Add year
    orders = orders.withColumn(
        "purchase_year",
        year("order_purchase_timestamp")
    )

    # Add month
    orders = orders.withColumn(
        "purchase_month",
        month("order_purchase_timestamp")
    )

    # Add day of week
    orders = orders.withColumn(
        "purchase_day_of_week",
        dayofweek("order_purchase_timestamp")
    )

    # Calculate delivery days
    orders = orders.withColumn(
        "delivery_days",
        datediff(
            "order_delivered_customer_date",
            "order_purchase_timestamp"
        )
    )

    # Check whether delivery was late
    orders = orders.withColumn(
        "is_late",
        when(
            col("order_delivered_customer_date")
            > col("order_estimated_delivery_date"),
            True
        ).otherwise(False)
    )

    # Remove duplicate orders
    orders = orders.dropDuplicates(["order_id"])

    logger.info("Orders cleaned successfully.")

    return orders


# ============================================================
# 5. CLEAN PRODUCTS
# ============================================================

def clean_products(products, category_translation):

    logger.info("Cleaning products...")

    # Replace NULL values
    products = products.fillna({
        "product_category_name": "unknown",
        "product_weight_g": 0,
        "product_length_cm": 0,
        "product_height_cm": 0,
        "product_width_cm": 0,
        "product_photos_qty": 0
    })

    # Join category translation
    products = products.join(
        broadcast(category_translation),
        on="product_category_name",
        how="left"
    )

    logger.info("Products cleaned successfully.")

    return products


# ============================================================
# 6. AGGREGATE PAYMENTS
# ============================================================

def aggregate_payments(payments):

    logger.info("Aggregating payments...")

    payments = payments.filter(
        col("payment_value") > 0
    )

    payment_summary = (
        payments
        .groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_payment"),
            count("payment_sequential").alias("payment_count"),
            collect_set("payment_type").alias("payment_types")
        )
    )

    return payment_summary


# ============================================================
# 7. AGGREGATE ORDER ITEMS
# ============================================================

def aggregate_order_items(order_items):

    logger.info("Aggregating order items...")

    items_summary = (
        order_items
        .groupBy("order_id")
        .agg(
            count("order_item_id").alias("item_count"),
            sum("price").alias("items_total"),
            sum("freight_value").alias("freight_total")
        )
    )

    # Calculate order total
    items_summary = items_summary.withColumn(
        "order_total",
        col("items_total") + col("freight_total")
    )

    return items_summary


# ============================================================
# 8. BUILD MASTER TABLE
# ============================================================

def build_master_table(
    orders,
    order_items,
    customers,
    payments
):

    logger.info("Building master table...")

    master = (
        orders
        .join(order_items, "order_id", "left")
        .join(customers, "customer_id", "left")
        .join(payments, "order_id", "left")
    )

    # Replace NULL financial values
    master = master.withColumn(
        "order_total",
        coalesce(col("order_total"), lit(0))
    )

    master = master.withColumn(
        "total_payment",
        coalesce(col("total_payment"), lit(0))
    )

    logger.info(
        f"Master table rows: {master.count()}"
    )

    return master


# ============================================================
# 9. MAIN PIPELINE
# ============================================================

def main():

    logger.info("=" * 50)
    logger.info("OLIST PYSPARK PIPELINE STARTED")
    logger.info("=" * 50)

    spark = create_spark()

    try:

        # Step 1: Read raw data
        (
            orders,
            order_items,
            customers,
            products,
            payments,
            category_translation
        ) = read_data(spark)

        # Step 2: Clean orders
        orders_clean = clean_orders(orders)

        # Step 3: Clean products
        products_clean = clean_products(
            products,
            category_translation
        )

        # Step 4: Aggregate payments
        payments_clean = aggregate_payments(
            payments
        )

        # Step 5: Aggregate order items
        items_clean = aggregate_order_items(
            order_items
        )

        # Step 6: Build master table
        master = build_master_table(
            orders_clean,
            items_clean,
            customers,
            payments_clean
        )

        # ====================================================
        # 10. CACHE MASTER TABLE
        # ====================================================

        logger.info("Caching master table...")

        master.cache()

        # Trigger cache
        master.count()

        # ====================================================
        # 11. WRITE MASTER TABLE
        # ====================================================

        output_path = (
            f"{PROCESSED_PATH}/master_orders"
        )

        logger.info(
            f"Writing output to: {output_path}"
        )

        (
            master
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        logger.info(
            "Master table written successfully."
        )

        # ====================================================
        # 12. SIMPLE REPORT
        # ====================================================

        report_path = (
            f"{PROCESSED_PATH}/orders_summary"
        )

        (
            master
            .select(
                "order_id",
                "order_status",
                "purchase_year",
                "purchase_month",
                "delivery_days",
                "is_late",
                "item_count",
                "order_total",
                "total_payment",
                "customer_state"
            )
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(report_path)
        )

        logger.info(
            "Order summary created successfully."
        )

        # Free cache
        master.unpersist()

        logger.info("=" * 50)
        logger.info("OLIST PYSPARK PIPELINE COMPLETED")
        logger.info("=" * 50)

    except Exception as error:

        logger.exception(
            f"Pipeline failed: {error}"
        )

        raise

    finally:

        spark.stop()

        logger.info(
            "Spark session stopped."
        )


# ============================================================
# 13. RUN
# ============================================================

if __name__ == "__main__":
    main()
