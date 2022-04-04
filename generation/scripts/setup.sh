#!/usr/bin/env bash

cd ..

cp -r src/generators ../Docker/airflow/dags
mv ../Docker/airflow/dags/generators ../Docker/airflow/dags/utils

cp -r src/dags/* ../Docker/airflow/dags

mkdir ../Docker/airflow/dags/tmp