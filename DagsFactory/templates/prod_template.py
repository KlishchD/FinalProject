import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator

with DAG(dag_id='$dag_id', start_date=pendulum.parse('2020/01/01'), schedule_interval=$schedule_interval) as dag:
    configs = load_configs('$configs_filepath')

    run_job = DataprocSubmitSparkJobOperator(
        task_id='run_job',
        region='$region',
        gcp_conn_id='$gcp_connection_id',
        arguments=["$job_name", "prod", "$spark_app_name"] + configs,
        main_class='$job_filepath'
    )
