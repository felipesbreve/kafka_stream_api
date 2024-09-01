from datetime import datetime
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import json
import requests # type: ignore
from kafka import KafkaProducer # type: ignore
import time
import logging

url = 'https://randomuser.me/api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1, 0, 0)
}

def get_data():
    response = requests.get(url)
    response = response.json()['results'][0]
    return response

def format_data(response):
    data = {}
    location = response['location']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['medium']
    return data

def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 5:
            break
        
        try:
            response = get_data()
            response = format_data(response)
            producer.send('users_created', key='key', value=response)

        except Exception as e:
            logging.error(f'Erro: {e}')
            continue

    # print(json.dumps(response, indent=3))

dag = DAG(
    'user_automation',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
)

streaming_task = PythonOperator(
    task_id = 'stream_data_from_api',
    python_callable = stream_data,
    dag = dag
)

streaming_task
