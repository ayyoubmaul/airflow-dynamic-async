from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def mapping_country():
    data = pd.read_csv('/opt/data/worldcities.csv')
    data = data.fillna(0)

    data_grouped = data.groupby('country')['population'].apply(list).to_dict()
    data_mapped = [{k: v} for k, v in data_grouped.items()]

    return data_mapped

def sum_population(data):
    for k, v in data.items():
        print(f'Total population of {k} is {sum(v)}')

with DAG(
    dag_id="dynamic_country_task_mapping_old",
    start_date=datetime(2023, 9, 28),
    catchup=False,
):
    map_country = PythonOperator(
        task_id='map_country',
        python_callable=mapping_country
    )

    sum_country_population = PythonOperator.partial(
        task_id='sum_country_population',
        python_callable=sum_population
    ).expand(op_kwargs=map_country.output)

    sum_country_population
