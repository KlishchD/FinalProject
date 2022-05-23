# Copy aggregational dags
cp dags/aggregations/dev/dags/*.py ../Docker/airflow/dags/aggregations/dags
cp dags/aggregations/run_configs/*.json ../Docker/airflow/dags/aggregations/configs
cp dags/aggregations/requirements.txt ../Docker/airflow/dags/aggregations/
cp -r Utils ../Docker/airflow/dags/aggregations/dags

# Copy preprocessing dags
cp dags/preprocessing/dev/dags/*.py ../Docker/airflow/dags/preprocessing/dags
cp dags/preprocessing/run_configs/*.json ../Docker/airflow/dags/preprocessing/configs
cp dags/preprocessing/requirements.txt ../Docker/airflow/dags/preprocessing/
cp -r Utils ../Docker/airflow/dags/preprocessing/dags
