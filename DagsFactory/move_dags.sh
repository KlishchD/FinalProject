# Copy aggregational dags
cp dags/aggregations/dev/dags/*.py ../Docker/airflow/dags/aggregations/dags
cp dags/aggregations/run_configs/*.json ../Docker/airflow/dags/aggregations/configs
cp -r Utils ../Docker/airflow/dags/aggregations/dags

# Copy preprocessing dags
cp dags/preprocessing/dev/dags/*.py ../Docker/airflow/dags/preprocessing/dags
cp dags/preprocessing/run_configs/*.json ../Docker/airflow/dags/preprocessing/configs
cp -r Utils ../Docker/airflow/dags/preprocessing/dags
