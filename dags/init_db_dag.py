from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'initialize_databases',
    default_args=default_args,
    description='Initialize database tables',
    schedule_interval=None,
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

def initialize_source_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_1')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            customer_email VARCHAR(255) NOT NULL,
            order_date TIMESTAMP NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(3) NOT NULL
        )
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders (order_date)
        """)

        conn.commit()
        print("Source database initialized successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing source database: {e}")
    finally:
        cursor.close()
        conn.close()

def initialize_destination_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_2')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders_eur (
            order_id UUID PRIMARY KEY,
            customer_email VARCHAR(255) NOT NULL,
            order_date TIMESTAMP NOT NULL,
            original_amount DECIMAL(10, 2) NOT NULL,
            original_currency VARCHAR(3) NOT NULL,
            eur_amount DECIMAL(10, 2) NOT NULL,
            exchange_rate DECIMAL(10, 6) NOT NULL,
            conversion_date TIMESTAMP NOT NULL
        )
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_eur_order_date ON orders_eur (order_date)
        """)

        conn.commit()
        print("Destination database initialized successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing destination database: {e}")
    finally:
        cursor.close()
        conn.close()

init_source_task = PythonOperator(
    task_id='initialize_source_db',
    python_callable=initialize_source_db,
    dag=dag,
)

init_dest_task = PythonOperator(
    task_id='initialize_destination_db',
    python_callable=initialize_destination_db,
    dag=dag,
)

[init_source_task, init_dest_task]
