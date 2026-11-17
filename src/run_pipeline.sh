#!/bin/bash
set -e

spark-submit src/jobs/s3_job.py
spark-submit src/jobs/snowflake_job.py
spark-submit src/jobs/api_job.py
spark-submit src/jobs/master_job.py

echo "AWS Retail Data Pipeline completed successfully."
