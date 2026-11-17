# AWS Retail Data Pipeline

An AWS-oriented PySpark batch pipeline that combines customer activity, customer scores, and API enrichment into a unified customer master dataset.

## Architecture

```text
Amazon S3 (raw customer activity CSV)
              |
           S3 Job ----------------------+
                                        |
Snowflake customer scores -> Snowflake Job
                                        |
Customer enrichment API ----> API Job --+
                                        |
                              Silver datasets in S3
                                        |
                                    Master Job
                                        |
                              Gold master dataset in S3
```

The jobs are designed for **Apache Spark on AWS EMR**. Amazon S3 stores the raw, Silver, and Gold datasets. AWS credentials should be supplied through the EMR instance profile / IAM role, not hardcoded.

## Important verification note

The cleaning and aggregation logic follows the CSV-based transformations that were previously executed successfully and produced 10,000 master customer records. That validation was performed in Databricks using CSV fixtures.

This repository is an **AWS/EMR deployment adaptation**. The S3, Snowflake connector, and live API integrations must be configured and tested in your AWS environment before claiming that the AWS deployment itself has been verified.

## Project structure

```text
aws_retail_data_pipeline/
├── config/
│   └── config.py
├── src/
│   ├── jobs/
│   │   ├── s3_job.py
│   │   ├── snowflake_job.py
│   │   ├── api_job.py
│   │   └── master_job.py
│   └── run_pipeline.sh
├── requirements.txt
├── .gitignore
└── README.md
```

## S3 data layout

Set `S3_BUCKET` to your bucket name. The pipeline uses:

```text
s3://<bucket>/raw/s3_customer_ip_activity/
s3://<bucket>/silver/silver_s3/
s3://<bucket>/silver/silver_snowflake/
s3://<bucket>/silver/silver_api/
s3://<bucket>/gold/master_customer_data/
```

The raw S3 activity CSV must contain `username` and `ip` columns.

## Environment configuration

Configure these environment variables in the EMR job environment:

- `S3_BUCKET`
- `SNOWFLAKE_URL`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_ROLE` (optional)
- `SNOWFLAKE_TABLE` (defaults to `CUSTOMER_SCORES`)
- `CUSTOMER_ENRICHMENT_API_URL`
- `CUSTOMER_ENRICHMENT_API_TOKEN` (optional)

The Snowflake source table is expected to contain `USERNAME` and `SCORE` columns. The API is expected to return either a JSON array of objects or an object with a `data` array. Each enrichment object must contain `username`, `version`, and `nationality`.

Use AWS Secrets Manager or another approved secret-injection method for credentials. Never commit passwords, API tokens, AWS keys, `.env` files, or real customer data to GitHub.

## Dependencies and EMR setup

Install Python dependencies from `requirements.txt`. The Snowflake **Spark Connector JAR/package** must also be configured on the EMR cluster and must be compatible with the EMR Spark and Scala versions. Installing the Python Snowflake connector alone does not provide the Spark data source used by this project.

## Run

Upload the repository to the EMR primary node or another location accessible to the cluster. Configure the environment variables, then run the jobs in order:

```bash
spark-submit src/jobs/s3_job.py
spark-submit src/jobs/snowflake_job.py
spark-submit src/jobs/api_job.py
spark-submit src/jobs/master_job.py
```

Alternatively:

```bash
bash src/run_pipeline.sh
```

Run the first three jobs before the Master Job.

## Output

The Master Job writes Parquet data to:

```text
s3://<bucket>/gold/master_customer_data/
```

Expected output columns:

- `username`
- `ip_count`
- `score_count`
- `average_score`
- `version`
- `nationality`

The earlier CSV-based test returned 10,000 customer rows. Re-run validation against the configured AWS sources after deployment.
