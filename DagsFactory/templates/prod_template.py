import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator, \
    DataprocCreateClusterOperator, DataprocDeleteClusterOperator

with DAG(dag_id='$dag_id', start_date=pendulum.parse('2020/01/01'), schedule_interval=$schedule_interval) as dag:
    configs = load_configs('$configs_filepath')

    create_cluster = DataprocCreateClusterOperator(
        task_id="set_up_temporary_cluster",
        cluster_config=$cluster_config,
        region='$region',
        gcp_conn_id='$gcp_connection_id',
        cluster_name="$job_name_temporary_cluster"
    )

    run_job = DataprocSubmitSparkJobOperator(
        task_id='run_job',
        region='$region',
        gcp_conn_id='$gcp_connection_id',
        arguments=["$job_name", "$spark_master", "$spark_app_name", "prod"] + configs,
        main_class='$job_filepath',
        cluster_name="$job_name_temporary_cluster"
    )

    remove_cluster = DataprocDeleteClusterOperator(
        task_id='remove_temporary_cluster',
        cluster_name="$job_name_temporary_cluster",
        region="$region"
    )

    create_cluster >> run_job >> remove_cluster

#
#
#
# $spark_master
#
#
# }
