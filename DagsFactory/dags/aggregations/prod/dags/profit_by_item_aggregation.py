import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator, \
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator

with DAG(dag_id='profit_by_item_aggregation', start_date=pendulum.parse('2020/01/01'), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/aggregations/profit_by_item_aggregation_config.json')

    create_cluster = DataprocCreateClusterOperator(
        task_id="set_up_temporary_cluster",
        cluster_config={'master_config': {'num_instances': 1, 'machine_type_uri': 'n1-standard-4', 'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}}, 'worker_config': {'num_instances': 2, 'machine_type_uri': 'n1-standard-4', 'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 512}}},
        region='europe-central2',
        gcp_conn_id='gcp_dataproc_connection',
        cluster_name="tempcluster"
    )

    run_job = DataprocSubmitSparkJobOperator(
        task_id='run_job',
        region='europe-central2',
        gcp_conn_id='gcp_dataproc_connection',
        arguments=["profitByItem", "$spark_master", "profit_by_item", "prod"] + configs,
        main_class='/usr/local/airflow/dags/aggregations/aggregations.jar',
        cluster_name="tempcluster"
    )

    remove_cluster = DataprocDeleteClusterOperator(
        task_id='remove_temporary_cluster',
        cluster_name="tempcluster",
        region="europe-central2"
    )

    create_cluster >> run_job >> remove_cluster