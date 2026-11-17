from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.config import API_URL, API_TOKEN, SILVER_API

spark = SparkSession.builder.appName("AWS-Retail-API-Enrichment-Job").getOrCreate()

# Fetch customer enrichment records from the configured API
headers = {}
if API_TOKEN:
    headers["Authorization"] = f"Bearer {API_TOKEN}"

response = requests.get(API_URL, headers=headers, timeout=60)
response.raise_for_status()
records = response.json()

# Accept a JSON array or an object containing a "data" array
if isinstance(records, dict):
    records = records.get("data", [])

if not isinstance(records, list) or not records:
    raise ValueError("API must return a non-empty JSON array or a 'data' array")

# Convert API records into a Spark DataFrame
df = spark.createDataFrame(records)

# Preserve username exactly because it is the customer join key
cleaned_df = df.select(
    F.trim(F.col("username")).alias("username"),
    F.col("version").cast("string").alias("version"),
    F.trim(F.col("nationality")).alias("nationality")
)

cleaned_df = cleaned_df.dropna(subset=["username", "nationality"])
cleaned_df = cleaned_df.filter(
    (F.col("username") != "") & (F.col("nationality") != "")
)

# Keep one enrichment record per customer
result = cleaned_df.dropDuplicates(["username"])

# Save the Silver dataset to Amazon S3
result.write.mode("overwrite").parquet(SILVER_API)

print("API Enrichment Job completed")
print("Total customers:", result.count())

spark.stop()
