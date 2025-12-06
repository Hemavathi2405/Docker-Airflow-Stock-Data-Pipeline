import os
import logging
from datetime import datetime
from typing import List, Dict

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create a PostgreSQL connection using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        return conn
    except Exception as e:
        logger.exception("Failed to connect to PostgreSQL")
        raise


def ensure_table(cursor):
    """Create the stock_prices table if it does not exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        price_date DATE NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        created_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(symbol, price_date)
    );
    """
    cursor.execute(create_table_sql)


def fetch_stock_data(symbol: str, api_key: str) -> List[Dict]:
    """
    Fetch daily stock data from Alpha Vantage for a given symbol.

    Returns a list of dicts:
    [
      {
        "symbol": "IBM",
        "price_date": date,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
      },
      ...
    ]
    """
    logger.info(f"Fetching data for symbol: {symbol}")

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    )

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.exception(f"Network error while fetching data for {symbol}")
        return []

    try:
        data = response.json()
    except ValueError:
        logger.error(f"Failed to decode JSON for {symbol}")
        return []

    # Handle API throttling / errors gracefully
    if "Note" in data:
        logger.warning(f"API limit note for {symbol}: {data['Note']}")
        return []
    if "Error Message" in data:
        logger.error(f"API error for {symbol}: {data['Error Message']}")
        return []

    time_series = data.get("Time Series (Daily)")
    if not time_series:
        logger.warning(f"No 'Time Series (Daily)' found for {symbol}")
        return []

    rows = []
    for date_str, values in time_series.items():
        try:
            price_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            open_price = float(values.get("1. open", 0)) if values.get("1. open") else None
            high_price = float(values.get("2. high", 0)) if values.get("2. high") else None
            low_price = float(values.get("3. low", 0)) if values.get("3. low") else None
            close_price = float(values.get("4. close", 0)) if values.get("4. close") else None
            volume = int(values.get("5. volume", 0)) if values.get("5. volume") else None

            # Skip if critical fields are missing
            if close_price is None:
                logger.debug(f"Skipping {symbol} {date_str}: missing close price")
                continue

            rows.append(
                {
                    "symbol": symbol,
                    "price_date": price_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                }
            )
        except Exception as e:
            logger.exception(f"Error parsing data for {symbol} on {date_str}")
            continue

    logger.info(f"Fetched {len(rows)} rows for {symbol}")
    return rows


def upsert_stock_data(rows: List[Dict]):
    """Insert/update rows into the stock_prices table."""
    if not rows:
        logger.info("No rows to upsert.")
        return

    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                ensure_table(cur)

                values = [
                    (
                        r["symbol"],
                        r["price_date"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                    )
                    for r in rows
                ]

                insert_sql = """
                INSERT INTO stock_prices (symbol, price_date, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, price_date) DO UPDATE
                SET
                    open  = EXCLUDED.open,
                    high  = EXCLUDED.high,
                    low   = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """

                execute_values(cur, insert_sql, values)
        logger.info(f"Upserted {len(rows)} rows into stock_prices.")
    except Exception as e:
        logger.exception("Error during upsert to PostgreSQL")
        # Let Airflow handle retries by re-raising
        raise
    finally:
        if conn:
            conn.close()


def fetch_and_store_stock_data():
    """
    Main function called by the Airflow DAG.
    1. Read env vars
    2. Fetch data for each symbol
    3. Upsert into PostgreSQL
    """
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is not set")

    symbols_csv = os.environ.get("STOCK_SYMBOLS", "IBM")
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    all_rows: List[Dict] = []
    for symbol in symbols:
        try:
            rows = fetch_stock_data(symbol, api_key)
            all_rows.extend(rows)
        except Exception:
            # Already logged inside fetch_stock_data; continue with other symbols
            logger.error(f"Skipping symbol {symbol} due to errors.")

    if not all_rows:
        logger.warning("No data fetched for any symbol. Nothing to store.")
        return

    upsert_stock_data(all_rows)
