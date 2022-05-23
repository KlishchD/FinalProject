import pendulum
from Utils.ParametersLoader import load_configs, load_raw_file
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='purchases_preprocessing', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/preprocessing/configs/purchases_preprocessing_config.json')
    requirements = load_raw_file('/usr/local/airflow/dags/preprocessing/requirements.txt')
    run_options = ["/usr/local/airflow/dags/preprocessing/preprocessing.jar",
                   "purchases",
                   "local[*]",
                   "purchases",
                   "-m dev"] + configs

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages {','.join(requirements)}  {' '.join(run_options)}"
    )
