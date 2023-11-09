"""
Streaming data consumer
"""
from datetime import datetime

import mysql.connector

from kafka.consumer import KafkaConsumer  # type: ignore

TOPIC = "toll"

config = {
    "host": "localhost",
    "user": "airflow",
    "password": "airflow",
    "database": "tolldata",
}

print("Connecting to the database")

db = mysql.connector.connect(**config)
cursor = db.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")

for msg in consumer:
    # Extract information from kafka
    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, "%a %b %d %H:%M:%S %Y")
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table
    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    db.commit()
db.close()
