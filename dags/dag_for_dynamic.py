import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id="dag_for_dynamic_generators", start_date=pendulum.parse("2020/10/10"), schedule_interval=None) as dag:
    generate = PythonOperator(
        task_id="generate",
        python_callable=lambda: ()
    )

