import requests 
import json
import time 
from datetime import datetime
from airflow import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 27),
}

def get_data():
    """
    this function gets data from the API, and returns the clean data (result only)
    """
    request = requests.get("https://randomuser.me/api/")
    raw_data = request.json()
    data = raw_data['results'][0]
    return data 

def format_data(data):
    """
    this function formats the data into a json string
    """
    formatted_data = {}

    formatted_data['first_name'] = data['name']['first']
    formatted_data['last_name'] = data['name']['last']
    formatted_data['gender'] = data['gender']
    formatted_data['address'] = f"{data['location']['street']['number']} {data['location']['street']['name']}, {data['location']['city']}, {data['location']['state']}, {data['location']['country']}, {data['location']['postcode']}"
    formatted_data['postcode'] = data['location']['postcode']
    formatted_data['email'] = data['email']
    formatted_data['username'] = data['login']['username']
    formatted_data['date_of_birth'] = data['dob']['date']
    formatted_data['age'] = data['dob']['age']
    formatted_data['phone'] = data['phone']
    formatted_data['picture'] = data['picture']['large']

    return formatted_data

def stream_data():
    """
    stream data into kafka queue 
    """
    raw_data = get_data()
    data = format_data(raw_data)

    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], max_block_ms = 5000) 

    """ push to the queue """
    producer.send("user_created", json.dumps(data).encode('utf-8')) 
     
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