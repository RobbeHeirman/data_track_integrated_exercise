from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="robbe-dag",
    description="Weather data from API to S3.",
    default_args={"owner": "Robbe Heirman"},
    schedule_interval="@daily",
    start_date=dt.datetime(2023, 12, 1),
)

with dag:
    ingest = BatchOperator(
        task_id="robbe-datatrack-ingest",
        job_name="robbe-datatrack-ingest",
        job_queue="integrated-exercise-job-queue",
        job_definition="robbe-datatrack-2",
        overrides={
            'command': [
                "python", "/src/integratedexercise/ingest.py",
                "-d","{{ ds }}",
                "-e","data-track-integrated-exercise"
            ]}
    )
