import json

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


def load_configs_for_generator(filepath):
    try:
        parsed = json.load(open(filepath))
    except FileNotFoundError:
        return ""

    result = ""

    for key, value in parsed.items():
        result += " --" + key + " " + str(value)

    return result[1:]


with DAG(dag_id='dag_for_static_generators', start_date=pendulum.parse("2020/10/10"), schedule_interval=None) as dag:
    parameters = load_configs_for_generator("/usr/local/airflow/dags/static_config.json")

    generate_data_task = BashOperator(
        task_id="generate",
        bash_command=f"cd /usr/local/airflow/dags/utils && python3 static_generator.py {parameters}"
    )
