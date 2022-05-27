#!/usr/bin/env bash

cp src/generators/* ../Docker/airflow/dags/generation/generators
cp -r src/generators/* ../Docker/airflow/dags/generation/generators
cp src/dags/scripts/*.sh ../Docker/airflow/dags/generation/scripts
cp src/dags/*.py ../Docker/airflow/dags/generation/dags
