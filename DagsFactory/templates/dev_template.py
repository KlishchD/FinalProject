import pendulum
from Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='$dag_id', start_date=pendulum.parse("2020/01/01"), schedule_interval=$schedule_interval) as dag:
    configs = load_configs('$configs_filepath')

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit $job_filepath $job_name $spark_master $spark_app_name dev {' '.join(configs)}"
    )
#{
# dag_id
# schedule_interval
# configs_filepath
# job_filepath
# job_name
# spark_master
# spark_app_name
#}