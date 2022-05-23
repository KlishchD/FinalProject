import pendulum
from Utils.ParametersLoader import load_configs, load_raw_file
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='grouped_items_aggregation', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/aggregations/configs/grouped_items_aggregation_config.json')
    requirements = load_raw_file('/usr/local/airflow/dags/aggregations/requirements.txt')
    run_options = ["/usr/local/airflow/dags/aggregations/aggregations.jar",
                   "groupedItems",
                   "local[*]",
                   "grouped_items",
                   "-m dev"] + configs

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages {','.join(requirements)}  {' '.join(run_options)}"
    )
