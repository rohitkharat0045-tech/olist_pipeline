from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
    lit,
    lower,
    trim,
    to_date,
    to_timestamp,
    when
)


# --------------------------------------------------
# Project paths
# --------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]

INPUT_PATH = ROOT_DIR / "processed" / "master_orders"

SQL_DIR = ROOT_DIR / "sql"

REPORTS_DIR = ROOT_DIR / "reports"


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Olist-Day4-SQL-Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n" + "=" * 70)
print("OLIST DAY 4 - SQL ANALYTICS PIPELINE")
print("=" * 70)


# --------------------------------------------------
# Read processed Parquet
# --------------------------------------------------

print("\n[1] Reading master_orders parquet...")

df = spark.read.parquet(
    str(INPUT_PATH)
)

print("Rows:", df.count())

print("\nSchema:")

df.printSchema()


# --------------------------------------------------
# Helper function
# --------------------------------------------------

def find_column(candidates, required=True):

    for column_name in candidates:

        if column_name in df.columns:

            return column_name

    if required:

        raise ValueError(
            f"\nCould not find any of these columns:\n"
            f"{candidates}\n\n"
            f"Available columns:\n{df.columns}"
        )

    return None


# --------------------------------------------------
# Detect important columns
# --------------------------------------------------

order_col = find_column([
    "order_id"
])

customer_col = find_column([
    "customer_unique_id",
    "customer_id"
])

status_col = find_column([
    "order_status",
    "status"
])

purchase_col = find_column([
    "order_purchase_timestamp",
    "purchase_timestamp",
    "order_date"
])

state_col = find_column([
    "customer_state",
    "state"
])

revenue_col = find_column([
    "total_payment_value",
    "payment_value",
    "total_payment",
    "total_order_payment",
    "total_paid",
    "order_total",
    "revenue"
])


# --------------------------------------------------
# Optional delivery columns
# --------------------------------------------------

delivery_days_col = find_column(
    [
        "delivery_days"
    ],
    required=False
)

late_col = find_column(
    [
        "is_late"
    ],
    required=False
)

delivered_date_col = find_column(
    [
        "order_delivered_customer_date"
    ],
    required=False
)

estimated_date_col = find_column(
    [
        "order_estimated_delivery_date"
    ],
    required=False
)


# --------------------------------------------------
# Create delivery_days
# --------------------------------------------------

if delivery_days_col:

    delivery_days_expression = (
        col(delivery_days_col)
        .cast("int")
    )

elif delivered_date_col:

    delivery_days_expression = datediff(
        to_date(col(delivered_date_col)),
        to_date(col(purchase_col))
    )

else:

    delivery_days_expression = (
        lit(None)
        .cast("int")
    )


# --------------------------------------------------
# Create is_late
# --------------------------------------------------

if late_col:

    late_text = lower(
        trim(
            col(late_col).cast("string")
        )
    )

    late_expression = (
        when(
            late_text.isin(
                "true",
                "1",
                "yes",
                "y"
            ),
            lit(True)
        )
        .when(
            late_text.isin(
                "false",
                "0",
                "no",
                "n"
            ),
            lit(False)
        )
        .otherwise(
            col(late_col).cast("boolean")
        )
    )

elif delivered_date_col and estimated_date_col:

    late_expression = (

        to_timestamp(
            col(delivered_date_col)
        )

        >

        to_timestamp(
            col(estimated_date_col)
        )

    )

else:

    late_expression = (
        lit(None)
        .cast("boolean")
    )


# --------------------------------------------------
# Standardize master table
# --------------------------------------------------

print("\n[2] Creating analytics dataset...")


analytics_df = (

    df.select(

        col(order_col)
        .alias("order_id"),

        col(customer_col)
        .alias("customer_id"),

        col(status_col)
        .alias("order_status"),

        to_timestamp(
            col(purchase_col)
        )
        .alias("order_purchase_timestamp"),

        col(state_col)
        .alias("customer_state"),

        col(revenue_col)
        .cast("double")
        .alias("revenue"),

        delivery_days_expression
        .alias("delivery_days"),

        late_expression
        .alias("is_late")
    )

    .filter(
        col("order_id").isNotNull()
    )

    .dropDuplicates([
        "order_id"
    ])

)


print(
    "Order-level rows:",
    analytics_df.count()
)


# --------------------------------------------------
# Register Temp View
# --------------------------------------------------

analytics_df.createOrReplaceTempView(
    "analytics_orders"
)

print(
    "\nTemporary view created:"
    " analytics_orders"
)


# --------------------------------------------------
# Execute views.sql
# --------------------------------------------------

print("\n[3] Creating SQL views...")


views_file = SQL_DIR / "views.sql"


with open(
    views_file,
    "r",
    encoding="utf-8"
) as file:

    views_sql = (
        file.read()
        .strip()
        .rstrip(";")
    )


spark.sql(
    views_sql
)


print(
    "Created:"
    " delivery_analysis_vw"
)


# --------------------------------------------------
# Reports
# --------------------------------------------------

reports = {

    "order_status":
        "01_order_status.sql",

    "monthly_revenue":
        "02_monthly_revenue.sql",

    "state_performance":
        "03_state_performance.sql",

    "running_revenue":
        "04_running_revenue.sql",

    "delivery_analysis":
        "05_delivery_analysis.sql"
}


# --------------------------------------------------
# Execute SQL reports
# --------------------------------------------------

print("\n[4] Running analytics reports...")


for report_name, sql_filename in reports.items():

    print(
        f"\n{'-' * 70}"
    )

    print(
        f"Running report: {report_name}"
    )


    sql_path = (
        SQL_DIR /
        sql_filename
    )


    with open(
        sql_path,
        "r",
        encoding="utf-8"
    ) as file:

        query = (
            file.read()
            .strip()
            .rstrip(";")
        )


    result_df = spark.sql(
        query
    )


    print("\nPreview:")

    result_df.show(
        20,
        truncate=False
    )


    output_path = (
        REPORTS_DIR /
        f"{report_name}_report"
    )


    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option(
            "header",
            True
        )
        .csv(
            str(output_path)
        )
    )


    print(
        f"Saved -> {output_path}"
    )


# --------------------------------------------------
# Finish
# --------------------------------------------------

print("\n" + "=" * 70)

print(
    "DAY 4 SQL ANALYTICS COMPLETED SUCCESSFULLY"
)

print("=" * 70)


spark.stop()
