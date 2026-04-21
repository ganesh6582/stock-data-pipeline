import json

import requests
import pandas as pd
import boto3
from datetime import datetime
import io

bucket_name = "de-stock-data-ganesh"



def fetch_data(url):
    headers = {
    "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
def extract_timestamps_and_prices(data):
    timestamps = data['chart']['result'][0]['timestamp']
    prices = data['chart']['result'][0]['indicators']['quote'][0]['close']
    return timestamps, prices
    
def save_bronze_data(data, bucket_name, file_name, stock_name):
    s3 = boto3.client('s3')
    json_buffer = io.StringIO()
    timestamps, prices = extract_timestamps_and_prices(data)
    df_raw = pd.DataFrame({'timestamp': timestamps, 'price': prices, 'stock': stock_name})
    df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], unit='s')
    df_raw.to_json(json_buffer, orient='records', lines=True)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_buffer.getvalue())
    return f"s3://{bucket_name}/{file_name}"

def process_data(data, stock_name, now):
    timestamps, prices = extract_timestamps_and_prices(data)
    df = pd.DataFrame({'timestamp': timestamps, 'price': prices, 'stock': stock_name})
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['ingestion_date'] = now.date()
    df = df.dropna()
    return df

def save_to_s3(df, bucket_name, file_name):
    s3 = boto3.client('s3')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    return f"s3://{bucket_name}/{file_name}"

STOCK_LIST = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS"]

def main():

    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    processed_data_file_name = f"processed/year={year}/month={month}/day={day}/stock_data_{now.strftime('%Y%m%d%H%M%S')}.csv"
    # create empty list to store dataframes for each stock
    dfs = []

    # Fetch and move raw data to S3, then process and combine data for all stocks
    for stock in STOCK_LIST:
        print("Fetching data for:", stock)
        try:
            data = fetch_data(f"https://query1.finance.yahoo.com/v8/finance/chart/{stock}")
        except Exception as e:
            print(f"Error fetching data for {stock}: {e}")
            continue
        raw_data_file_name = f"raw/year={year}/month={month}/day={day}/{stock}_{now.strftime('%Y%m%d%H%M%S')}.json"
        s3_path = save_bronze_data(data, bucket_name, raw_data_file_name, stock.split('.')[0])
        df = process_data(data, stock.split('.')[0], now)  # Use the stock name without the .NS suffix
        print("Processed data for:", stock)
        dfs.append(df)

    if not dfs:
        raise Exception("No data fetched")
    print("Saving combined data to S3...")
    combined_df = pd.concat(dfs, ignore_index=True)
    print("Combined DataFrame shape:", combined_df.shape)
    s3_path = save_to_s3(combined_df, bucket_name, processed_data_file_name)
    print(f"Data saved to: {s3_path}")

def lambda_handler(event, context):
    main()
    return {
        "statusCode": 200,
        "body": "Pipeline executed successfully"
    }