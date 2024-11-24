from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import datetime


file1=Dataset("/tmp/file1.txt")
file2=Dataset("/tmp/file2.txt")
with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2024,11,23),
    catchup=False) as dag:


    @task(outlets=[file1])
    def update_file():
        with open(file1.uri ,"a+") as file:
            file.write("producer update")

    @task(outlets=[file2])
    def update_file():
        with open(file2.uri ,"a+") as file:
            file.write("producer update1")
    
    
    update_file()
        