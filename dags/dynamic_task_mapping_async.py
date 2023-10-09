import asyncio

from datetime import datetime
from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.operators.dummy_operator import DummyOperator
import os
import pandas as pd


DAG_ID = 'collect_weather_data'

TMP_STORAGE_LOCATION = f'/tmp/{DAG_ID}'

WEATHER_API_KEY = os.environ['FINNHUB_API_KEY']

DATA_DIR = '/data'
USE_COLS = ['city_ascii', 'lat', 'lng', 'country', 'iso2', 'iso3']

BATCH_SIZE = 100


def print_output(cities_data):
    print(f'Cities output weather {cities_data}')

def read_data():
    filename = f'{DATA_DIR}/worldcities.csv'

    # keep_default_na because of iso2 of Namibia = NA
    world_cities = pd.read_csv(filename, usecols=USE_COLS, keep_default_na=False)
    # keep the number of requests small
    world_cities = world_cities.sample(400)

    data = world_cities.to_dict(orient='records')

    result = [
        {"batch": data[start : start + BATCH_SIZE]}
        for start in range(0, len(data), BATCH_SIZE)
    ]

    return result

async def run(async_hook, endpoint, params, i):
    print(f'process {i}')

    response = await async_hook.run(endpoint=endpoint, data=params)
    json_response = await response.json()

    await asyncio.sleep(10)

    print(f'finished {i}')

    return json_response


def __batch_api_requests(batch, **context):
    print('Start async requests')

    async_hook = HttpAsyncHook(method="GET", http_conn_id="OWM_API")
    loop = asyncio.get_event_loop()

    coroutines = []

    for i, city in enumerate(batch):
        endpoint = "data/2.5/weather"

        params = {"appid": WEATHER_API_KEY, "lat": city["lat"], "lon": city["lng"]}

        coroutines.append(run(async_hook, endpoint, params, i))

    data = loop.run_until_complete(asyncio.gather(*coroutines))

    return data


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 9, 24),
    schedule="0 0 * * *",
    render_template_as_native_obj=True,
    tags=['dynamic task mapping'],
):
    read_csv = PythonOperator(
        task_id="read_csv",
        python_callable=read_data,
    )

    http_requests = PythonOperator.partial(
        task_id="http_requests",
        python_callable=__batch_api_requests
    ).expand(op_kwargs=read_csv.output)

    finish = DummyOperator(
        task_id = 'finish'
    )

    http_requests >> finish
