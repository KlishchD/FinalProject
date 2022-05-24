import pendulum
from Utils.ParametersLoader import load_configs, load_raw_file
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='location_sell_number_aggregation', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/aggregations/configs/location_sell_number_share_aggregation_config.json')
    requirements = load_raw_file('/usr/local/airflow/dags/aggregations/requirements.txt')
    run_options = ["/usr/local/airflow/dags/aggregations/aggregations.jar",
                   "locationsSellShare",
                   "local[*]",
                   "location_sell_number",
                   "-m dev"] + configs

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages {','.join(requirements)}  {' '.join(run_options)}"
    )
