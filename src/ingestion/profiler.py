import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from src.utils.logger import get_logger


# ============================================================
# 1. LOAD CONFIGURATION
# ============================================================

load_dotenv("config.env")

RAW_PATH = os.getenv("RAW_PATH", "./raw")
REPORTS_PATH = os.getenv("REPORTS_PATH", "./reports")

logger = get_logger("profiler")


# ============================================================
# 2. FILE MAPPING
# ============================================================

FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}


# ============================================================
# 3. EXPECTED SCHEMAS
# ============================================================

EXPECTED_SCHEMAS = {

    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],

    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ],

    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ],

    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ],

    "sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ],

    "payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    ],

    "reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ],

    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ],

    "category_translation": [
        "product_category_name",
        "product_category_name_english"
    ]
}


# ============================================================
# 4. SAFE CSV READER
# ============================================================

def safe_read(filepath):

    try:

        df = pd.read_csv(
            filepath,
            low_memory=False
        )

        logger.info(
            f"Loaded: {filepath} | "
            f"{df.shape[0]} rows | "
            f"{df.shape[1]} columns"
        )

        return df

    except FileNotFoundError:

        logger.error(
            f"File not found: {filepath}"
        )

        return None

    except pd.errors.EmptyDataError:

        logger.error(
            f"Empty file: {filepath}"
        )

        return None

    except pd.errors.ParserError as error:

        logger.error(
            f"CSV parsing error in {filepath}: {error}"
        )

        return None

    except Exception as error:

        logger.error(
            f"Unexpected error while reading {filepath}: {error}"
        )

        return None

# ============================================================
# 5. PROFILE DATAFRAME
# ============================================================

def profile_dataframe(df, table_name):

    null_counts = (
        df.isnull()
        .sum()
        .to_dict()
    )

    null_percentage = (
        (
            df.isnull().sum()
            / len(df)
            * 100
        )
        .round(2)
        .to_dict()
    )

    duplicate_rows = int(
        df.duplicated().sum()
    )

    memory_mb = round(
        df.memory_usage(
            deep=True
        ).sum()
        / 1024**2,
        2
    )

    profile = {

        "table": table_name,

        "rows": int(
            len(df)
        ),

        "columns": int(
            df.shape[1]
        ),

        "column_names": list(
            df.columns
        ),

        "data_types": {
            column: str(dtype)
            for column, dtype
            in df.dtypes.items()
        },

        "null_counts": {
            key: int(value)
            for key, value
            in null_counts.items()
        },

        "null_percentage": {
            key: float(value)
            for key, value
            in null_percentage.items()
        },

        "duplicate_rows": duplicate_rows,

        "memory_mb": memory_mb
    }

    logger.info(
        f"{table_name} | "
        f"Rows: {len(df)} | "
        f"Duplicates: {duplicate_rows} | "
        f"Nulls: {sum(null_counts.values())}"
    )

    return profile

# ============================================================
# 6. VALIDATE SCHEMA
# ============================================================

def validate_schema(
    df,
    table_name,
    expected_columns
):

    actual_columns = set(
        df.columns
    )

    expected_columns = set(
        expected_columns
    )

    missing_columns = (
        expected_columns
        - actual_columns
    )

    extra_columns = (
        actual_columns
        - expected_columns
    )

    if missing_columns:

        logger.warning(
            f"{table_name} missing columns: "
            f"{missing_columns}"
        )

    if extra_columns:

        logger.info(
            f"{table_name} extra columns: "
            f"{extra_columns}"
        )

    is_valid = (
        len(missing_columns) == 0
    )

    logger.info(
        f"{table_name} schema valid: "
        f"{is_valid}"
    )

    return {
        "schema_valid": is_valid,
        "missing_columns": list(
            missing_columns
        ),
        "extra_columns": list(
            extra_columns
        )
    }
# ============================================================
# 7. MAIN PROFILER
# ============================================================

def main():

    logger.info("=" * 60)
    logger.info("OLIST DATA PROFILER STARTED")
    logger.info("=" * 60)

    os.makedirs(
        REPORTS_PATH,
        exist_ok=True
    )

    all_profiles = {}

    for table_name, filename in FILES.items():

        logger.info(
            f"Profiling table: {table_name}"
        )

        filepath = os.path.join(
            RAW_PATH,
            filename
        )

        # ----------------------------------------
        # Read file
        # ----------------------------------------

        df = safe_read(
            filepath
        )

        if df is None:

            logger.error(
                f"Skipping {table_name}"
            )

            continue

        # ----------------------------------------
        # Profile data
        # ----------------------------------------

        profile = profile_dataframe(
            df,
            table_name
        )

        # ----------------------------------------
        # Validate schema
        # ----------------------------------------

        if table_name in EXPECTED_SCHEMAS:

            schema_result = (
                validate_schema(
                    df,
                    table_name,
                    EXPECTED_SCHEMAS[
                        table_name
                    ]
                )
            )

            profile.update(
                schema_result
            )

        all_profiles[
            table_name
        ] = profile

    # ========================================================
    # SAVE JSON REPORT
    # ========================================================

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    report_path = os.path.join(
        REPORTS_PATH,
        f"raw_profile_{timestamp}.json"
    )

    with open(
        report_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            all_profiles,
            file,
            indent=4,
            default=str
        )

    logger.info("=" * 60)

    logger.info(
        f"Profile report saved: "
        f"{report_path}"
    )

    logger.info(
        "OLIST DATA PROFILER COMPLETED"
    )

    logger.info("=" * 60)


# ============================================================
# 8. RUN PROGRAM
# ============================================================

if __name__ == "__main__":
    main()
