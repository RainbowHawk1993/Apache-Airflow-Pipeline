from datetime import datetime, timedelta
import uuid
import random
import string
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'generate_orders',
    default_args=default_args,
    description='Generate orders data and insert into postgres-1 every 10 minutes',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

def fetch_currencies():
    api_key = Variable.get("openexchangerates_api_key")
    url = f"https://openexchangerates.org/api/currencies.json?app_id={api_key}"

    response = requests.get(url)
    currencies = response.json()

    return list(currencies.keys())

def generate_email():
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'protonmail.com']
    name_length = random.randint(5, 10)
    name = ''.join(random.choices(string.ascii_lowercase, k=name_length))
    domain = random.choice(domains)
    return f"{name}@{domain}"

def generate_orders():
    pg_hook = PostgresHook(postgres_conn_id='postgres_1')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        currencies = fetch_currencies()
        common_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SEK', 'NZD']
        currencies = [c for c in common_currencies if c in currencies]
        if not currencies:
            currencies = common_currencies
    except Exception as e:
        print(f"Error fetching currencies: {e}")
        currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SEK', 'NZD']  # Default if API fails

    orders = []
    now = datetime.now()

    for _ in range(5000):
        order_id = uuid.uuid4()
        customer_email = generate_email()
        days_ago = random.randint(0, 6)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        order_date = now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        amount = round(random.uniform(10.0, 1000.0), 2)
        currency = random.choice(currencies)

        orders.append((order_id, customer_email, order_date, amount, currency))

    try:
        insert_query = """
        INSERT INTO orders (order_id, customer_email, order_date, amount, currency)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, orders)
        conn.commit()
        print(f"Successfully inserted {len(orders)} orders")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting orders: {e}")
    finally:
        cursor.close()
        conn.close()

generate_task = PythonOperator(
    task_id='generate_orders',
    python_callable=generate_orders,
    dag=dag,
)

generate_task
