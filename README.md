# Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

## Objective

This project implements a **Dockerized data pipeline** using **Apache Airflow** to automatically
fetch, parse, and store stock market data from a free API (Alpha Vantage) into a PostgreSQL database.

The pipeline:

- Fetches JSON stock market data on a **scheduled basis** (hourly).
- Parses the JSON response and updates a PostgreSQL table.
- Handles errors and missing data **gracefully**.
- Uses **Docker Compose** to run everything with one command.

---

## Architecture

**Components:**

- **Airflow** – Orchestrator running in a Docker container
- **PostgreSQL** – Database to store stock prices
- **Docker Compose** – Single command to bring up the whole stack
- **Python scripts** – Logic for fetching and storing data

**Data flow:**

1. Airflow DAG triggers **hourly**.
2. DAG calls the function `fetch_and_store_stock_data()` from `fetch_and_store.py`.
3. The function:
   - Calls Alpha Vantage API using `requests`
   - Parses the JSON response
   - Upserts data into PostgreSQL using `psycopg2` (with `ON CONFLICT` for idempotency)

---

## Prerequisites

- Docker
- Docker Compose
- A free [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key)

---

## Setup & Run

### Clone the project (or copy files) & Build and run it up

```bash
git clone <your-repo-url> #To clone the repository 
cd stock-pipeline #Point to project directory
docker compose up --build #To build and run the project
docker exec -it stock-pipeline-postgres-1 psql -U stocks_user -d stocks_db #To opens the PostgreSQL database inside your Docker container
SELECT * FROM stock_prices ORDER BY price_date DESC LIMIT 20; #To Show the latest 20 rows from the stock_prices table