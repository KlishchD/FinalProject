#!/usr/bin/env bash


cp -r ../generators ../Docker/airflow/dags
mv ../Docker/airflow/dags/generators ../Docker/airflow/dags/utils

cp -r ../dags/* ../Docker/airflow/dags

mkdir ../Docker/airflow/dags/tmp