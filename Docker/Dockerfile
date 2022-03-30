FROM apache/airflow:latest
RUN pip install pandas

ENV GOOGLE_APPLICATION_CREDENTIALS='/usr/local/airflow/dags/gd-gcp-gridu-cloud-cert-bd717a1f2832.json'
ENV AIRFLOW_HOME='/usr/local/airflow'