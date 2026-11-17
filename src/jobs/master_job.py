from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.config import SILVER_S3, SILVER_SNOWFLAKE, SILVER_API, GOLD_MASTER

spark = SparkSession.builder.appName("AWS-Retail-Master-Job").getOrCreate()

# Read the three Silver datasets from Amazon S3
s3_df = spark.read.parquet(SILVER_S3)
snowflake_df = spark.read.parquet(SILVER_SNOWFLAKE)
api_df = spark.read.parquet(SILVER_API)

# Join S3 activity and Snowflake customer scores
master_df = s3_df.join(
    snowflake_df,
    on="username",
    how="left"
)

# Enrich the customer master data with API fields
master_df = master_df.join(
    api_df,
    on="username",
    how="left"
)

# Save the final Gold dataset to Amazon S3
master_df.write.mode("overwrite").parquet(GOLD_MASTER)

print("Master Job completed")
print("Total customers:", master_df.count())

master_df.orderBy(F.desc("average_score")).show(20, truncate=False)

spark.stop()
