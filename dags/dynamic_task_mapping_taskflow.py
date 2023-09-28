from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd


@dag(schedule="@daily", start_date=datetime(2023, 9, 28), catchup=False)
def dynamic_country_task_mapping():
    @task
    def mapping_country():
        data = pd.read_csv('/opt/data/worldcities.csv')
        data = data.fillna(0)

        data_grouped = data.groupby('country')['population'].apply(list).to_dict()
        data_mapped = [{k: v} for k, v in data_grouped.items()]

        return data_mapped

    @task
    def sum_population(data):
        for k, v in data.items():
            print(f'Total population of {k} is {sum(v)}')

    sum_population.expand(data=mapping_country())

dynamic_country_task_mapping()
