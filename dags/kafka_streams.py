from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import uuid
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner':'Frank',
    'start_date': datetime(2024, 3, 8, 10, 00)
}

def get_data():
    
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    # print(res)
    return res


def format_data(res):
    
    data = {}
    location = res['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                        f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    print(data)
    return data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    cur_time = time.time()
    
    while True:
        if time.time() > cur_time +60: #1 min
            break
        try:      
            res = get_data()
            res = format_data(res)
            # print(json.dumps(res, indent=3))  
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            
        except Exception as e:
            logging.error(f"an error occured: {e}")
            continue
            
    
    

with DAG(
    dag_id = 'streaming_user_data_dag',
    default_args = default_args,
    schedule = '@daily',
    catchup = True
    ) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )

