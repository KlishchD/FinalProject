from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from Utils.ParametersLoader import load_configs

with DAG(dag_id=$dag_id, start_date=pendulum.parse("01/01/2020"), schedule_interval=$schedule_interval) as dag:
    configs = load_configs($configs_filepath)

    run_job = DataprocSubmitJobOperator(
        task_id="run_job",
        job=$job,
        region=$region
    )
