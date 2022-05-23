import pendulum
from Utils.ParametersLoader import load_configs, load_raw_file
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='$dag_id', start_date=pendulum.parse("2020/01/01"), schedule_interval=$schedule_interval) as dag:
    configs = load_configs('$configs_filepath')
    requirements = load_raw_file('$requirements')
    run_options = ["$job_filepath",
                   "$job_name",
                   "$spark_master",
                   "$spark_app_name",
                   "-m dev"] + configs

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages {','.join(requirements)}  {' '.join(run_options)}"
    )
