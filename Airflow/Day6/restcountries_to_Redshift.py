from airflow import DAG
from airflow.models import Variable
from airflow.porviders.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
import requests
import logging

def get_Redshift_connection(autocommit = True):
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    logging.info(datetime.utcnow())
    req = requests.get(url)
    json_data = req.json()
    return json_data

@task
def transform(data):
    records = []
    for record in data:
        name = record["name"]["official"]
        population = record["population"]
        area = record["area"]
        records.append([name, population, area])
    logging.info("Transform ended")
    return records

@task
def load(schema, table, records):
    logging.info("Load start")
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} (name VARCHAR(255), population INT, area FLOAT);")

        insert_sql = f"INSERT INTO {schema}.{table} (name, population, area) VALUES (%s, %s, %s)"
        for record in records:
            cur.execute(insert_sql, record)

        cur.execute("COMMIT;")            
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
        cur.execute("ROLLBACK;")
    logging.info("Load Done")


with DAG(
    dag_id='restcountries'
    start_date=datetime(2025,1,15),
    schedule='30 6 * * 6',
    max_active_run=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
) as dag:
    url = Variable.get('restcountries_url')
    schema = 'eunzy093'
    table = 'restcountries'

    records = transform(extract(url))
    load(schema, table, records)