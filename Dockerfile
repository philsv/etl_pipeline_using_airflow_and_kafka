FROM apache/airflow:latest

USER root

RUN pip install -r requirements.txt

USER airflow