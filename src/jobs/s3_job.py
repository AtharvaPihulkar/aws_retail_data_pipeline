from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.config import RAW_S3_ACTIVITY, SILVER_S3

spark = SparkSession.builder.appName("AWS-Retail-S3-Activity-Job").getOrCreate()

# Read raw customer IP activity from Amazon S3
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_S3_ACTIVITY)
)

# Clean username and IP address
cleaned_df = df.select(
    F.trim(F.col("username")).alias("username"),
    F.trim(F.col("ip")).alias("ip")
)

cleaned_df = cleaned_df.dropna(subset=["username", "ip"])
cleaned_df = cleaned_df.filter(
    (F.col("username") != "") & (F.col("ip") != "")
)

# Count IP activity records per customer
result = cleaned_df.groupBy("username").agg(
    F.count("ip").alias("ip_count")
)

# Save the Silver dataset to Amazon S3
result.write.mode("overwrite").parquet(SILVER_S3)

print("S3 Activity Job completed")
print("Total customers:", result.count())

spark.stop()
