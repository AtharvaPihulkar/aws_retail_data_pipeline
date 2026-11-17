from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.config import SNOWFLAKE_OPTIONS, SNOWFLAKE_TABLE, SILVER_SNOWFLAKE

spark = SparkSession.builder.appName("AWS-Retail-Snowflake-Job").getOrCreate()

# Read customer scores from Snowflake through the Snowflake Spark Connector
df = (
    spark.read
    .format("net.snowflake.spark.snowflake")
    .options(**SNOWFLAKE_OPTIONS)
    .option("dbtable", SNOWFLAKE_TABLE)
    .load()
)

# Clean usernames and score values
cleaned_df = df.select(
    F.trim(F.col("USERNAME")).alias("username"),
    F.col("SCORE").cast("double").alias("score")
)

cleaned_df = cleaned_df.dropna(subset=["username", "score"])
cleaned_df = cleaned_df.filter(F.col("username") != "")

# Calculate score count and average score per customer
result = cleaned_df.groupBy("username").agg(
    F.count("score").alias("score_count"),
    F.avg("score").alias("average_score")
)

# Save the Silver dataset to Amazon S3
result.write.mode("overwrite").parquet(SILVER_SNOWFLAKE)

print("Snowflake Job completed")
print("Total customers:", result.count())

spark.stop()
