import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator

with DAG(dag_id='grouped_items_aggregation', start_date=pendulum.parse('2020/01/01'), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/aggregations/grouped_items_aggregation_config.json')

    run_job = DataprocSubmitSparkJobOperator(
        task_id='run_job',
        region='europe-central2',
        gcp_conn_id='gcp_dataproc_connection',
        arguments=["groupedItems", "prod", "grouped_items"] + configs,
        main_class='/usr/local/airflow/dags/aggregations/aggregations.jar'
    )
