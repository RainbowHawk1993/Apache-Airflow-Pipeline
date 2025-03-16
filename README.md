# Airflow Data Pipeline

This project implements a data pipeline using Apache Airflow to:
- Generate order data every 10 minutes and store it in a PostgreSQL database
- Transfer and convert the order data to EUR currency on an hourly basis

# Setup Instructions
## Prerequisites

- Docker and Docker Compose installed
- Free OpenExchangeRates API key (sign up at https://openexchangerates.org/)
- Make sure ports 5432 and 8080 are available on your machine

## Getting Started

Start the Docker containers:
```
docker-compose up -d
```
Wait for webserver to start (should take around ~5 minutes), you can check health status by running `docker-compose ps`

Access the Airflow web interface at http://localhost:8080

Login with name and password `admin`

### Set your OpenExchangeRates API key in Airflow:
Go to Admin > Variables

Set the `variable openexchangerates_api_key` with your API key

### Running DAGs
- Run the `initialize_databases` DAG manually to set up the database tables
- Turn on `generate_orders` and `transfer_orders_to_eur` DAGs

You can check if databases get populated correctly by running the following commands:
```
docker exec -it NAME_FOR_POSTGRES-1_CONTAINER_HERE -U airflow -d orders_db -c "SELECT COUNT(*) FROM orders;"
docker exec -it NAME_FOR_POSTGRES-2_CONTAINER_HERE -U airflow -d orders_eur_db -c "SELECT COUNT(*) FROM orders_eur;"
```

## Things to improve
`orders` table should have a boolean column that gets switched to true once an order gets converted to EUR, so that it doesn't get selected for future runs of `transfer_orders_to_eur` DAG
