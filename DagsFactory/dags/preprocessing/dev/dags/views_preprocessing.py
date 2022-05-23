import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='views_preprocessing', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/preprocessing/configs/views_preprocessing_config.json')

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages de.halcony:scala-argparse_2.13:1.1.11,org.postgresql:postgresql:42.3.3 /usr/local/airflow/dags/preprocessing/preprocessing.jar views local[*] views -m dev {' '.join(configs)}"
    )
