from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from airflow.models import Variable

@dag(
    schedule="*/15 * * * *",           
    start_date=datetime(2025, 6, 30),
    catchup=False,
    tags=['ploomes', 'api', 'pipeline']
)
def ploomes_tasks_pipeline():

    @task
    def fetch_and_save_tasks():
        load_dotenv()
        url = "https://api2.ploomes.com/Tasks"
        user_key = Variable.get("user_key")
        headers = {'User-Key': user_key, 'Content-Type': 'application/json'}

        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Erro na API: status {response.status_code}")
        data = response.json()
        deals = data.get("value", [])
        all_deals = []
        all_deals.extend(deals)

        df = pd.json_normalize(all_deals)
        df.to_excel('/usr/local/airflow/dags/tasks.xlsx', index=False)
        print("Arquivo tasks.xlsx salvo com sucesso.")

    fetch_and_save_tasks()

ploomes_tasks_pipeline()

