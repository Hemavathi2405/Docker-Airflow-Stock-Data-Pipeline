#!/bin/bash

echo "Initializing Airflow DB..."
airflow db init
airflow db upgrade

echo "Creating Admin user if not exists..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

echo "Starting Airflow Webserver and Scheduler..."
airflow scheduler &

exec airflow webserver
