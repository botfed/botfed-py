import pandas as pd
import os
from binance.client import Client
import datetime
import dotenv

dotenv.load_dotenv()

# Replace with your Binance API key and secret
api_key = os.getenv("BIN_KEY")
api_secret = os.getenv("BIN_SECRET")

# Initialize Binance Client
client = Client(api_key, api_secret)


def get_funding_payments(start_date, end_date, limit=1000):
    """Fetches funding payments from Binance Futures account for a given period."""

    # Convert start and end dates to timestamp (milliseconds)
    start_timestamp = int(
        datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000
    )
    end_timestamp = int(
        datetime.datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000
    )

    # Fetch funding payments using the income endpoint
    funding_payments = []
    try:
        result = client.futures_income_history(
            startTime=start_timestamp,
            endTime=end_timestamp,
            incomeType="FUNDING_FEE",
            limit=limit,
        )
        funding_payments.extend(result)
    except Exception as e:
        print(f"Error fetching funding payments: {e}")
        return pd.DataFrame()  # Return empty DataFrame if error occurs

    # Convert to a Pandas DataFrame
    if len(funding_payments) > 0:
        df = pd.DataFrame(funding_payments)
        df["time"] = pd.to_datetime(
            df["time"], unit="ms"
        )  # Convert time from milliseconds to datetime
        return df
    else:
        print("No funding payments found for the given period.")
        return pd.DataFrame()


if __name__ == "__main__":

    # Example usage: fetch funding payments for a given date range
    start_date = "2024-06-01"  # Example start date
    end_date = "2024-10-08"  # Example end date
    df = get_funding_payments(start_date, end_date)
    df['income'] = df['income'].astype(float)
    df.to_csv("funding_payments.csv", index=False)  # Save to CSV file
    ena_funding = df[df['symbol'] == 'ENAUSDT']['income'].astype(float).sum()
    print(f"Total funding payments for ENAUSDT: {ena_funding}")
    print(f"Total funding payments for all symbols: {df['income'].astype(float).sum()}")
    # print dataframe with total payments per symbol:
    print(df.groupby('symbol')['income'].sum())
