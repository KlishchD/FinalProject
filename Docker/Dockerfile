FROM phusion/baseimage:focal-1.2.0

#install Java and Scala
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y scala


RUN apt-get install ca-certificates-java

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
#Done

#Install Airflow
RUN apt-get install pip -y

RUN pip install apache-airflow
RUN pip install apache-airflow-providers-hashicorp
RUN pip install apache-airflow-providers-postgres
RUN pip install apache-airflow-providers-celery
RUN pip install apache-airflow-providers-redis
RUN pip install apache-airflow-providers-google

ENV AIRFLOW_HOME=/usr/local/airflow
#Done

#Install Spark

WORKDIR /

RUN apt-get install wget -y && wget --no-verbose http://www.gtlib.gatech.edu/pub/apache/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2-scala2.13.tgz

RUN tar -xzf /spark-3.2.1-bin-hadoop3.2-scala2.13.tgz && \
    mv spark-3.2.1-bin-hadoop3.2-scala2.13 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc \

ENV SPARK_HOME=/spark/bin
ENV PATH=$PATH:/spark/bin

WORKDIR /opt/airflow

#Done

#Install additional packages

RUN pip install pandas

#Done

ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/dags/key.json

