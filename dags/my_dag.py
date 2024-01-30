import json
import pandas as pd

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    tags=["example"],
)
def my_dag():
    @task()
    def extract():
        # with open("dags/inputs/input.json") as f:
        #     json_data = json.load(f)
        csv_data = pd.read_csv("dags/inputs/input.csv")
        return csv_data

    @task()
    def transform(csv_data):
        obs_average = csv_data["Observation"].mean()
        return obs_average

    @task()
    def load(obs_average):
        print(obs_average)

    extracted_csv = extract()
    obs_average = transform(extracted_csv)
    load(obs_average)


my_dag = my_dag()
