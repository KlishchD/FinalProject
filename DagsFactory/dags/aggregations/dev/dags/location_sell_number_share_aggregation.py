import pendulum
from DagsFactory.Utils.ParametersLoader import load_configs
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id='location_sell_number_aggregation', start_date=pendulum.parse("2020/01/01"), schedule_interval=None) as dag:
    configs = load_configs('/usr/local/airflow/dags/aggregations/configs/location_sell_number_share_aggregation_config.json')

    run_job = BashOperator(
        task_id="run_job",
        bash_command=f"spark-submit --packages de.halcony:scala-argparse_2.13:1.1.11,org.postgresql:postgresql:42.3.3 /usr/local/airflow/dags/aggregations/aggregations.jar locationsSellShare local[*] location_sell_number -m dev {' '.join(configs)}"
    )