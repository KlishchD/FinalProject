#!/usr/bin/env bash

cd /usr/local/airflow/dags/utils
python3 static_generator.py $@ --items_filepath "/usr/local/airflow/dags/tmp/items.csv" --users_filepath "/usr/local/airflow/dags/tmp/users.csv" --ips_filepath "/usr/local/airflow/dags/tmp/ips.csv"