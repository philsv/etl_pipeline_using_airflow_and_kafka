import csv
import os
import tarfile
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

BASE_PATH = Path(__file__).parent
STAGING_DIR = BASE_PATH / "finalassignment" / "staging"

default_args = {
    "owner": "philsv",
    "start_date": datetime.today(),
    "email": "philsv@example.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule_interval=timedelta(days=1),
)


def download_and_extract_data() -> None:
    """
    Downloads and extracts data from the IBM Cloud Object Storage
    """
    os.chdir(STAGING_DIR)
    cloud_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud"
    file_path = "IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    url = f"{cloud_url}/{file_path}"
    urllib.request.urlretrieve(url, "tolldata.tgz")
    with tarfile.open("tolldata.tgz", "r:gz") as tar:
        tar.extractall(STAGING_DIR)


unzip_data = PythonOperator(
    task_id="unzip_data", python_callable=download_and_extract_data, dag=dag
)


def extract_data_from_csv_func() -> None:
    """
    Extracts data from vehicle-data.csv and writes it to csv_data.csv
    """
    with open(STAGING_DIR / "vehicle-data.csv", "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        with open(STAGING_DIR / "csv_data.csv", "w") as output_file:
            csv_writer = csv.writer(output_file)
            for row in csv_reader:
                # Extract Rowid, Timestamp, Anonymized Vehicle number and Vehicle type
                csv_writer.writerow(row[:4])


extract_data_from_csv = PythonOperator(
    task_id="extract_data_from_csv", python_callable=extract_data_from_csv_func, dag=dag
)


def extract_data_from_tsv_func() -> None:
    """
    Extracts data from tollplaza-data.tsv and writes it to tsv_data.csv
    """
    with open(STAGING_DIR / "tollplaza-data.tsv", "r") as tsv_file, \
         open(STAGING_DIR / "tsv_data.csv", "w") as output_file:
            for line in tsv_file:
                fields = line.strip().split("\t")
                # Extract Number of axles, Tollplaza id and Tollplaza code
                output_file.write(",".join(fields[4:7]) + "\n")


extract_data_from_tsv = PythonOperator(
    task_id="extract_data_from_tsv", python_callable=extract_data_from_tsv_func, dag=dag
)


def extract_data_from_fixed_width_func() -> None:
    """
    Extracts data from payment-data.txt and writes it to fixed_width_data.csv
    """
    with open(STAGING_DIR / "payment-data.txt", "r") as input_file, \
         open(STAGING_DIR / "fixed_width_data.csv", "w") as output_file:
        
        for line in input_file:
            fields = line.strip().split()
            # Extract Type of Payment code and Vehicle Code
            output_file.write(",".join(fields[10:12]) + "\n")


extract_data_from_fixed_width = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width_func,
    dag=dag,
)


def consolidate_data_func() -> None:
    """
    Consolidates data from csv_data.csv, tsv_data.csv, and fixed_width_data.csv
    and writes it to consolidated_data.csv
    """
    with open(STAGING_DIR / "csv_data.csv", "r") as csv_file, \
         open(STAGING_DIR / "tsv_data.csv", "r") as tsv_file, \
         open(STAGING_DIR / "fixed_width_data.csv", "r") as fixed_width_file, \
         open(STAGING_DIR / "consolidated_data.csv", "w") as output_file:
        
        csv_reader = csv.reader(csv_file, delimiter=",")
        tsv_reader = csv.reader(tsv_file, delimiter=",")
        fixed_width_reader = csv.reader(fixed_width_file, delimiter=",")

        for csv_row, tsv_row, fixed_width_row in zip(csv_reader, tsv_reader, fixed_width_reader):
            output_file.write(",".join(csv_row + tsv_row[1:] + fixed_width_row[1:]) + "\n")


consolidate_data = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data_func,
    dag=dag,
)


def transform_data_func() -> None:
    """
    Transforms data from extracted_data.csv and writes it to transformed_data.csv
    """
    with open(STAGING_DIR / "extracted_data.csv", "r") as input_file, \
         open(STAGING_DIR / "transformed_data.csv", "w") as output_file:
        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file)
    
        for row in csv_reader:
            row[2] = row[2].capitalize()
            csv_writer.writerow(row)


transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data_func,
    dag=dag,
)


# Task Pipeline
(
    unzip_data
    >> extract_data_from_csv
    >> extract_data_from_tsv
    >> extract_data_from_fixed_width
    >> consolidate_data
    >> transform_data
)
