#Move dags
cp src/dags/configs/*.json ../Docker/airflow/dags/generation/configs
cp src/dags/scripts/*.sh ../Docker/airflow/dags/generation/scripts
cp src/dags/*.py ../Docker/airflow/dags/generation/dags
cp -r src/dags/Utils ../Docker/airflow/dags/generation/dags

#Move generators
cp -r src/generators/* ../Docker/airflow/dags/generation/generators