# FinalProject

## Set up

1) Clone repository
2) Run set_up.sh script in the root

Woala now you have set up all infrastructure with all dags.

## Run 

1) run run.sh script

## Infrastructure

Infrastructure was built using Docker compose and has such services:

1) Airflow with Celery (port 8000, flower port 5555);
2) Postgres for storing results of aggregations (port 3030);
3) Adminer provides web based interface to interact with postgres (port 8080);
4) Redis to store results of preprocessing (port 6060);
5) Kafka (port 9092)


    
