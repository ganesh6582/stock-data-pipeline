import json
import requests
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
    timestamps, prices = extract_timestamps_and_prices(data)

    lines = []
    for ts, price in zip(timestamps, prices):
        lines.append(json.dumps({
            "timestamp": datetime.fromtimestamp(ts).isoformat(),
            "price": price,
            "stock": stock_name
        }))

    json_data = "\n".join(lines)

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data, ContentType='application/json')
    return f"s3://{bucket_name}/{file_name}"

def process_data(data, stock_name, now):
    timestamps, prices = extract_timestamps_and_prices(data)
    rows = []
    for ts, price in zip(timestamps, prices):
        if price is not None:
            rows.append(f"{datetime.fromtimestamp(ts)},{price},{stock_name},{now.date()}")
    csv_data = "timestamp,price,stock,ingestion_date\n" + "\n".join(rows)
    return csv_data

def save_to_s3(csv_data, bucket_name, file_name):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data, ContentType='text/csv')
    return f"s3://{bucket_name}/{file_name}"

def repair_athena_table():
    client = boto3.client('athena')

    query = "MSCK REPAIR TABLE stock_prices;"

    client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'stock_db'},
        ResultConfiguration={
            'OutputLocation': 's3://de-stock-data-ganesh/athena-results/'
        }
    )

STOCK_LIST = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS"]

def main():

    now = datetime.now()

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    processed_data_file_name = f"processed/year={year}/month={month}/day={day}/stock_data_{now.strftime('%Y%m%d%H%M%S')}.csv"
    # create empty list to store dataframes for each stock
    all_rows = []

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
        csv_data = process_data(data, stock.split('.')[0], now)  # Use the stock name without the .NS suffix
        print("Processed data for:", stock)
        # remove header except first time
        lines = csv_data.split("\n")
    
        if not all_rows:
            all_rows.extend(lines)  # include header
        else:
            all_rows.extend(lines[1:])  # skip header

    if not all_rows:
        raise Exception("No data fetched")
    print("Saving combined data to S3...")
    final_csv = "\n".join(all_rows)
    save_to_s3(final_csv, bucket_name, processed_data_file_name)
    repair_athena_table()

def lambda_handler(event, context):
    main()
    return {
        "statusCode": 200,
        "body": "Pipeline executed successfully"
    }