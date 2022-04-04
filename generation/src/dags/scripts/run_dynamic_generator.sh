#!/usr/bin/env bash

cd /usr/local/airflow/dags/utils
python3 dynamic_generator.py $@ --items_filepath "/usr/local/airflow/dags/tmp/items.csv" --users_filepath "/usr/local/airflow/dags/tmp/users.csv" --views_filepath "/usr/local/airflow/dags/tmp/views.csv" --purchases_filepath "/usr/local/airflow/dags/tmp/purchases.json"