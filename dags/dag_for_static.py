import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.ParametersLoader import load_configs_for_generator

with DAG(dag_id='dag_for_static_generators', start_date=pendulum.parse("2020/10/10"), schedule_interval=None) as dag:
    parameters = load_configs_for_generator("/usr/local/airflow/dags/static_config.json")

    generate_data_task = BashOperator(
        task_id="generate",
        bash_command=f"/usr/local/airflow/dags/scripts/run_static_generator.sh {parameters}"
    )
