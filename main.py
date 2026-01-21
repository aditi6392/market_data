import pandas as pd
import requests
from pymongo import MongoClient
import config


def read_tickers_from_csv(csv_filepath, ticker_column_name='SYMBOL'):

    try:
        df = pd.read_csv(csv_filepath)

        df.columns = (
            df.columns
            .str.strip()
            .str.replace('#', '', regex=False)
            .str.upper()
        )

        ticker_column_name = (
            ticker_column_name
            .strip()
            .replace('#', '')
            .upper()
        )

        if ticker_column_name not in df.columns:
            raise ValueError(
                f"Column '{ticker_column_name}' not found. "
                f"Available columns: {df.columns.tolist()}"
            )

        # Clean ticker values
        tickers = (
            df[ticker_column_name]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )

        return tickers

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []


def fetch_api_data(ticker):
    headers = {
        'accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json'
    }

    # API 1: Equity Quotes
    api_1_url = f"{config.API_QUOTES_URL}?symbols={ticker}"
    headers['Authorization'] = f"Bearer {config.AUTH_TOKEN_API_1}"
    api_1_response = requests.get(api_1_url, headers=headers, timeout=10)
    api_1_data = api_1_response.json() if api_1_response.status_code == 200 else {}
    print(f"API 1 response for {ticker}: {api_1_response.status_code}")

    # API 2: Quote Profile
    api_2_url = f"{config.API_QUOTE_PROFILE_URL}?symbol={ticker}"
    headers['Authorization'] = f"Bearer {config.AUTH_TOKEN_API_2}"
    api_2_response = requests.get(api_2_url, headers=headers, timeout=10)
    api_2_data = api_2_response.json() if api_2_response.status_code == 200 else {}
    print(f"API 2 response for {ticker}: {api_2_response.status_code}")

    # API 3: Fundamentals (POST)
    api_3_url = config.API_FUNDAMENTALS_URL
    headers['Authorization'] = f"Bearer {config.AUTH_TOKEN_API_3}"
    api_3_payload = {"symbol": ticker, "mode": "annual"}
    api_3_response = requests.post(
        api_3_url, headers=headers, json=api_3_payload, timeout=10
    )
    api_3_data = api_3_response.json() if api_3_response.status_code == 200 else {}
    print(f"API 3 response for {ticker}: {api_3_response.status_code}")

    return {
        "ticker": ticker,
        "quotes_data": api_1_data,
        "profile_data": api_2_data,
        "fundamentals_data": api_3_data
    }


def save_to_mongodb(data):
    try:
        client = MongoClient(config.MONGO_URI)
        db = client[config.MONGO_DB_NAME]
        collection = db[config.MONGO_COLLECTION_NAME]
        collection.insert_one(data)
        print(f"Data for {data['ticker']} saved to MongoDB.")
        client.close()
    except Exception as e:
        print(f"Error saving data to MongoDB: {e}")


def main():
    csv_filepath = r"E:\contract_master.csv"
    ticker_column = "SYMBOL"

    tickers = read_tickers_from_csv(csv_filepath, ticker_column)

    if not tickers:
        print("No tickers found or CSV file could not be read. Exiting.")
        return

    for ticker in tickers:
        print(f"\nProcessing ticker: {ticker}")
        merged_data = fetch_api_data(ticker)
        save_to_mongodb(merged_data)

    print("\nScript finished successfully.")


if __name__ == "__main__":
    main()
