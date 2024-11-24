from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import datetime


file1=Dataset("/tmp/file1.txt")
file2=Dataset("/tmp/file2.txt")

with DAG(
    dag_id="consumer",
    schedule=[file1,file2],
    start_date=datetime(2024,11,23),
    catchup=False) as dag:


    @task
    def read_data():
        with open(file1.uri,"r") as file:
            print(file.read())

    read_data()
