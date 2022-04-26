import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='views_preprocessing', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/preprocessing/views_preprocessing_config.json')

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit /usr/local/airflow/dags/preprocessing/preprocessing.jar views dev views {' '.join(configs)}"
    )
