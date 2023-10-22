import asyncio
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.operators.dummy_operator import DummyOperator
import logging
import os


DAG_ID = 'dynamic_task_mapping_async'
FINNHUB_API_KEY = os.environ['FINNHUB_API_KEY']

async def run(async_hook, endpoint, params, i):
    logging.info(f'process {i}')

    response = await async_hook.run(endpoint=endpoint, data=params)
    json_response = await response.json()

    await asyncio.sleep(10)

    logging.info(f'finished process {i}')

    logging.info(json_response)

    return json_response


def __batch_api_requests(batch, **context):
    logging.info('Start async requests')

    async_hook = HttpAsyncHook(method='GET', http_conn_id='FINNHUB_API')
    loop = asyncio.get_event_loop()

    coroutines = []

    # Create HTTP connection in Airflow with host : 'https://finnhub.io/'
    for i in range(5):
        endpoint = f'api/v1/news'
        params = {'category': batch[0], 'token': FINNHUB_API_KEY}

        coroutines.append(run(async_hook, endpoint, params, i))

    data = loop.run_until_complete(asyncio.gather(*coroutines))

    return data


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 10, 8),
    schedule='0 0 * * *',
    render_template_as_native_obj=True,
    tags=['dynamic task mapping async'],
):
    http_requests = PythonOperator.partial(
        task_id='http_requests',
        python_callable=__batch_api_requests
    ).expand(op_kwargs=[{'batch': [cat]} for cat in ['general', 'forex', 'crypto', 'merger']])

    finish = DummyOperator(
        task_id = 'finish'
    )

    http_requests >> finish
