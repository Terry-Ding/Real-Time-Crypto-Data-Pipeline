#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt 
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then 
    airflow db init && \
    airflow users create \
        --username "$AIRFLOW_ADMIN_USER" \
        --firstname "$AIRFLOW_ADMIN_FIRSTNAME" \
        --lastname "$AIRFLOW_ADMIN_LASTNAME" \
        --role Admin \
        --email "$AIRFLOW_ADMIN_EMAIL" \
        --password "$AIRFLOW_ADMIN_PASSWORD" 
fi 

$(command -v airflow) db upgrade

exec airflow webserver 