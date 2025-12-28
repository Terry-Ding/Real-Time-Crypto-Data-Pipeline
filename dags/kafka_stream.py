import os
import requests 
import json
from datetime import datetime
from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 27),
}

def stream_data():
    request = requests.get("https://randomuser.me/api/")
    raw_data = request.json()
    data = raw_data['results'][0]
    print(data)

# with DAG(
#     dag_id = "user_automation",
#     default_args = default_args,
#     schedule = "@daily",
# ) as dag:
#     streaming_task = PythonOperator(
#         task_id = "stream_data_from_api", 
#         python_callable = stream_data())

if __name__ == "__main__":
    stream_data()