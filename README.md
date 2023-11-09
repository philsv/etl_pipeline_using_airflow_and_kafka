# ETL Pipeline using Airflow and Kafka

Final assignment for the course "ETL and Data Pipelines with Shell, Airflow and Kafka" by IBM on Coursera.

## Author(s)

* Yan Luo
* Jeff Grossman
* Sabrina Spillner
* Ramesh Sannareddy

## Prerequisites

```bash
# Insert your DAG_PATH into your .env file.
cp .env.example .env

# Run airflow and mysql Database in docker
docker-compose up -d --build

# Download and install Kafka
cd kafka && bash get_kafka.sh
```
