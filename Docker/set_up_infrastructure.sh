# shellcheck disable=SC2164
cd airflow/dags

#Set up place for data
mkdir data

#Set up place for aggregational dags
mkdir aggregations
mkdir aggregations/configs
mkdir aggregations/dags

#Set up place for generational dags
mkdir generation
mkdir generation/configs
mkdir generation/dags
mkdir generation/generators
mkdir generation/scripts

#Set up place for preprocessing dags

mkdir preprocessing
mkdir preprocessing/configs
mkdir preprocessing/dags

cd ../..

docker-compose up --build

docker exec webserver airflow users create -r Admin -u admin -p admin -f admin -l admin -e admin@admin.com
