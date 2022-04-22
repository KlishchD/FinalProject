from airflow import DAG
from airflow.operators.bash import BashOperator
from Utils.ParametersLoader import load_configs
import pendulum

with DAG(dag_id=$dag_id, start_date=pendulum.parse("01/01/2020"), schedule_interval=$schedule_interval) as dag:
    configs = load_configs($configs_filepath)

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit {$job_filepath} {' '.join(configs)}"
    )
