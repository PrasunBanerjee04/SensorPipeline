from airflow import DAG
#from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import json

with DAG(
    dag_id='sensor_pipeline',
    start_date=days_ago(1),
    schedule_interval='0 */8 * * *',  # Every 8 hours at minute 0
    catchup=False
) as dag:
    
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            timestamp TIMESTAMP NOT NULL,
            result NUMERIC(10, 4),
            PRIMARY KEY (timestamp)
        );
        """
        postgres_hook.run(create_table_query)
    
    @task
    def extract_sensor_data():
        yesterday = datetime.utcnow().date() - timedelta(days=1)
        start_date = f"{yesterday}T00:00:00Z"
        end_date = f"{yesterday}T23:59:59Z"

        base_url = "https://api.sealevelsensors.org/v1.0/Datastreams(262)/Observations"
        url = (
            f"{base_url}"
            f"?$orderby=phenomenonTime%20desc"
            f"&$filter=phenomenonTime%20ge%20{start_date}"
            f"%20and%20phenomenonTime%20le%20{end_date}"
        )

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data

    @task
    def transform_sensor_data(response):
        observations = response.get("value", [])

        transformed_data = [
            {
                "timestamp": obs["phenomenonTime"],
                "result": float(obs["result"])
            }
            for obs in observations
            if "phenomenonTime" in obs and "result" in obs
        ]

        return transformed_data
    
    @task
    def load_data_to_postgres(sensor_data):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = """
        INSERT INTO sensor_data (timestamp, result)
        VALUES (%s, %s)
        ON CONFLICT (timestamp) DO NOTHING;
        """
        for data in sensor_data:
            postgres_hook.run(insert_query, parameters=(
                data['timestamp'],
                data['result']
            ))
    
    # Task flow
    create = create_table()
    raw_data = extract_sensor_data()
    transformed = transform_sensor_data(raw_data)
    load = load_data_to_postgres(transformed)

    create >> raw_data >> transformed >> load



    