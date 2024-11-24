from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize
from airflow.decorators import task
from datetime import datetime
import json

def _process_user(ti):
    user=ti.xcom_pull(task_ids="extract_user")
    user=user['results'][0]
    procesed_user=json_normalize({
        'firstname':user['name']['first'],
        'lastname':user['name']['last'],
        'country':user['location']['country'],
        'username':user['login']['username'],
        'password':user['login']['password'],
        'email':user['email']})

    procesed_user.to_csv("/tmp/processed_user.csv",index=None,header=False)


def _insert_user():
    hook=PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY USERS from stdin WITH DELIMITER as ',' ",
        filename="/tmp/processed_user.csv")


with DAG(dag_id="user_processing",
         schedule_interval='@daily',
         start_date=days_ago(1),
         catchup=False) as dag:
    
    create_table=SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS USERS (
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL);
            """
            )
    @task
    def is_api_aviliable():
        task_id="is_api_aviliable",
        http_conn_id="user_api",
        endpoint="api/"

    # is_api_aviliable=HttpSensor(
    #     task_id="is_api_aviliable",
    #     http_conn_id="user_api",
    #     endpoint="api/")
    
    extract_user=SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="api/",
        method='GET',
        response_filter=lambda response:json.loads(response.text),
        log_response=True)
    
    process_user=PythonOperator(
        task_id="process_user",
        python_callable=_process_user)
    
    insert_user=PythonOperator(
        task_id="insert_user",
        python_callable=_insert_user)
    
    create_table >> is_api_aviliable()>> extract_user >> process_user >>insert_user

    