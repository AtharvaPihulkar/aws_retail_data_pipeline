from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# Configure these Airflow Variables before enabling the DAG:
#   emr_cluster_id = j-XXXXXXXXXXXXX
#   s3_bucket = your-bucket-name
#
# Upload this project ZIP to:
#   s3://<s3_bucket>/code/aws_retail_data_pipeline_github.zip
# Upload the job scripts to:
#   s3://<s3_bucket>/code/src/jobs/<job_name>.py
#
# The project ZIP is supplied to Spark with --py-files so config/config.py
# is available to the job scripts.

EMR_CLUSTER_ID = Variable.get("emr_cluster_id")
S3_BUCKET = Variable.get("s3_bucket")

PROJECT_ZIP = (
    f"s3://{S3_BUCKET}/code/aws_retail_data_pipeline_github.zip"
)
JOB_BASE = f"s3://{S3_BUCKET}/code/src/jobs"


def emr_spark_step(step_name, script_name):
    return [{
        "Name": step_name,
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--py-files",
                PROJECT_ZIP,
                f"{JOB_BASE}/{script_name}",
            ],
        },
    }]


with DAG(
    dag_id="aws_retail_data_pipeline",
    description="Orchestrates S3, Snowflake, API, and Master PySpark jobs on AWS EMR",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "emr", "pyspark", "retail-data-pipeline"],
) as dag:

    add_s3_job = EmrAddStepsOperator(
        task_id="add_s3_activity_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=emr_spark_step("S3 Activity Job", "s3_job.py"),
        aws_conn_id="aws_default",
    )

    wait_s3_job = EmrStepSensor(
        task_id="wait_for_s3_activity_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='add_s3_activity_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 60,
    )

    add_snowflake_job = EmrAddStepsOperator(
        task_id="add_snowflake_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=emr_spark_step("Snowflake Job", "snowflake_job.py"),
        aws_conn_id="aws_default",
    )

    wait_snowflake_job = EmrStepSensor(
        task_id="wait_for_snowflake_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='add_snowflake_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 60,
    )

    add_api_job = EmrAddStepsOperator(
        task_id="add_api_enrichment_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=emr_spark_step("API Enrichment Job", "api_job.py"),
        aws_conn_id="aws_default",
    )

    wait_api_job = EmrStepSensor(
        task_id="wait_for_api_enrichment_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='add_api_enrichment_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 60,
    )

    add_master_job = EmrAddStepsOperator(
        task_id="add_master_job_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=emr_spark_step("Master Job", "master_job.py"),
        aws_conn_id="aws_default",
    )

    wait_master_job = EmrStepSensor(
        task_id="wait_for_master_job_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='add_master_job_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 60,
    )

    (
        add_s3_job
        >> wait_s3_job
        >> add_snowflake_job
        >> wait_snowflake_job
        >> add_api_job
        >> wait_api_job
        >> add_master_job
        >> wait_master_job
    )
