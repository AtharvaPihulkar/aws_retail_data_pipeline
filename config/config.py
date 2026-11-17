import os

S3_BUCKET = os.environ["S3_BUCKET"]

RAW_S3_ACTIVITY = f"s3a://{S3_BUCKET}/raw/s3_customer_ip_activity/"
SILVER_S3 = f"s3a://{S3_BUCKET}/silver/silver_s3/"
SILVER_SNOWFLAKE = f"s3a://{S3_BUCKET}/silver/silver_snowflake/"
SILVER_API = f"s3a://{S3_BUCKET}/silver/silver_api/"
GOLD_MASTER = f"s3a://{S3_BUCKET}/gold/master_customer_data/"

SNOWFLAKE_OPTIONS = {
    "sfURL": os.environ["SNOWFLAKE_URL"],
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}

if os.getenv("SNOWFLAKE_ROLE"):
    SNOWFLAKE_OPTIONS["sfRole"] = os.environ["SNOWFLAKE_ROLE"]

SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "CUSTOMER_SCORES")
API_URL = os.environ["CUSTOMER_ENRICHMENT_API_URL"]
API_TOKEN = os.getenv("CUSTOMER_ENRICHMENT_API_TOKEN")
