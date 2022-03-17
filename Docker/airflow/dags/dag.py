from airflow import DAG
from airflow.operators.dummy import DummyOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

with DAG(dag_id="id", start_date=pendulum.parse("2020-10-11"), schedule_interval=None) as dag:
    DummyOperator(task_id="dummy")

    globals()["dag"] = dag
