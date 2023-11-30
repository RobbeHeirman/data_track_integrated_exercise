from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

dag = DAG(

)

ingest = BatchOperator(
    task_id="ingest",
    job_name="robbe-datatrack-ingest",
    job_queue="integrated-exercise-job-queue",
    job_definition="robbe-datatrack-2",
    overrides=JOB_OVERRIDES,
)
