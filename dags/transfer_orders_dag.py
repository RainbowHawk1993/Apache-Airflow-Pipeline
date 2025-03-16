from datetime import datetime, timedelta
import requests
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
    'transfer_orders_to_eur',
    default_args=default_args,
    description='Transfer orders data from postgres-1 to postgres-2 and convert to EUR hourly',
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

def get_exchange_rates():
    """Fetch exchange rates from OpenExchangeRates with EUR as base"""
    api_key = Variable.get("openexchangerates_api_key")
    url = f"https://openexchangerates.org/api/latest.json?app_id={api_key}"

    try:
        response = requests.get(url)
        data = response.json()

        rates = data.get('rates', {})
        usd_to_eur = rates.get('EUR', 0)

        if usd_to_eur == 0:
            raise ValueError("Failed to get EUR exchange rate")

        eur_rates = {}
        for currency, rate in rates.items():
            eur_rates[currency] = rate / rates['EUR']

        return eur_rates
    except Exception as e:
        print(f"Error fetching exchange rates: {e}")
        # A fallback dictionary with some common rates
        return {
            'USD': 1.1,
            'EUR': 1.0,
            'GBP': 0.85,
            'JPY': 130.0,
            'CAD': 1.5,
            'AUD': 1.6,
            'CHF': 1.05,
            'CNY': 7.5,
            'SEK': 10.5,
            'NZD': 1.7
        }

def transfer_and_convert_orders():
    exchange_rates = get_exchange_rates()

    src_hook = PostgresHook(postgres_conn_id='postgres_1')
    src_conn = src_hook.get_conn()
    src_cursor = src_conn.cursor()

    dest_hook = PostgresHook(postgres_conn_id='postgres_2')
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    try:
        conversion_date = datetime.now()

        src_cursor.execute("""
        SELECT order_id, customer_email, order_date, amount, currency
        FROM orders
        """)
        #WHERE order_date >= current_date - INTERVAL '7 days'
        #AND order_date <= current_date
        #""")
        # Should be re-added if only orders from the last 7 days are needed

        orders = src_cursor.fetchall()

        if not orders:
            print("No new orders to transfer")
            return

        converted_orders = []
        for order_id, email, order_date, amount, currency in orders:
            exchange_rate = exchange_rates.get(currency, 1.0)

            eur_amount = round(float(amount) / exchange_rate, 2)

            converted_orders.append((
                order_id, email, order_date, amount, currency,
                eur_amount, exchange_rate, conversion_date
            ))

        dest_cursor.executemany("""
        INSERT INTO orders_eur (
            order_id, customer_email, order_date, original_amount,
            original_currency, eur_amount, exchange_rate, conversion_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
        """, converted_orders)

        dest_conn.commit()
        print(f"Successfully transferred and converted {len(converted_orders)} orders")

    except Exception as e:
        dest_conn.rollback()
        print(f"Error transferring and converting orders: {e}")

    finally:
        src_cursor.close()
        src_conn.close()
        dest_cursor.close()
        dest_conn.close()

transfer_task = PythonOperator(
    task_id='transfer_and_convert_orders',
    python_callable=transfer_and_convert_orders,
    dag=dag,
)

transfer_task
