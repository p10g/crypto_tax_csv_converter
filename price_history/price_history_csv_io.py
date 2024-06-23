import csv
import datetime
import os
import pandas as pd
import pytz
import requests
import sys

from io import TextIOWrapper
from typing import Any, Dict, Tuple

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
# Now you can import parent_module

# Use functions or classes from parent_module
from confidential import BIRDEYE_API_KEY

# Ticker, Address, Public name
# When adding a new token, you can look up this data on birdeye.so:
# Ticker: what's written in the trading pair, e.g.: RENDER/USD => the ticker is "RENDER"
# Address: written on the left panel on birdeye.so
# Public name: written on the left panel on birdeye.so
_TOKEN_DATA = [
    ("Mail", "C8cNX2D1y3jqKpMFkQhP1gGbfvTEdeckZXLBKSN5z5KF", "SolMail"),
    ("POPCAT", "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "POPCAT"),
    ("SOLC", "DLUNTKRQt7CrpqSX1naHUYoBznJ9pvMP65uCeWQgYnRK", "SolCard"),
    ("WHALES", "GTH3wG3NErjwcf7VGCoXEXkgXSHvYhx5gtATeeM5JAS1", "Whales Market"),
    ("RENDER", "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof", "Render Token"),
    ("JLP", "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4", "Jupiter Perps LP"),
]

_TICKER_TO_DATAFRAME: Dict[str, pd.DataFrame] = {}
COL_NAME_TICKER = "Ticker"
COL_NAME_ADDRESS = "Address"
COL_NAME_PUBLIC_NAME = "Public name"

COL_NAME_TIMESTAMP = "Timestamp"
COL_NAME_TIME = "Time"
COL_NAME_PRICE_IN_USD = "Price in USD"

def _open_or_create_file(token_data_entry: Tuple[str, str, str]) -> TextIOWrapper:
    ticker = token_data_entry[0]
    file_name = "price_history/data/" + ticker + ".csv"
    
    if not os.path.exists(file_name):
        with open(file_name, 'w+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow((COL_NAME_TICKER, COL_NAME_ADDRESS, COL_NAME_PUBLIC_NAME))
            csv_writer.writerow(token_data_entry)
            csv_writer.writerow((COL_NAME_TIMESTAMP, COL_NAME_TIME, COL_NAME_PRICE_IN_USD))
    
    return open(file_name, 'r+', newline='')

def _check_valid_csv(csv_reader) -> None:
    error_msg = "Invalid price history csv."
    
    line = next(csv_reader)
    if line[0] != COL_NAME_TICKER or line[1] != COL_NAME_ADDRESS or line[2] != COL_NAME_PUBLIC_NAME:
        raise Exception(error_msg)
    
    line = next(csv_reader)
    if line[0] == "" or line[1] == "" or line[2] == "":
        raise Exception(error_msg)
    
    line = next(csv_reader)
    if line[0] != COL_NAME_TIMESTAMP or line[1] != COL_NAME_TIME or line[2] != COL_NAME_PRICE_IN_USD:
        raise Exception(error_msg)

def _get_last_timestamp(csv_reader) -> int:
    timestamp_str = ""
    for line in csv_reader:
        timestamp_str = line[0]

    try:
        return int(timestamp_str)
    except ValueError:
        return 0

def _download_price_history(token_address: str, time_from: int) -> Any:
    time_from += 1
    time_to = int(datetime.datetime.now().timestamp())
    url = ("https://public-api.birdeye.so/defi/history_price?address=" + token_address + 
           "&address_type=token&type=4H&time_from=" + str(time_from) + 
           "&time_to=" + str(time_to))
    
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    
    response = requests.get(url, headers=headers)

    data = response.json()  # Can throw requests.exceptions.JSONDecodeError. Lets the caller handle it
    return data["data"]["items"]


def _load_csv_to_dataframes() -> None:
    for token_data in _TOKEN_DATA:
        ticker = token_data[0]
        file_name = "price_history/data/" + ticker + ".csv"
        df = pd.read_csv(file_name, skiprows=2, parse_dates=[COL_NAME_TIME])
        df.set_index(COL_NAME_TIMESTAMP, inplace=True)
        _TICKER_TO_DATAFRAME[ticker] = df

def update_price_history() -> None:
    for token_data_entry in _TOKEN_DATA:
        print("Updating price history for {}...".format(token_data_entry[0]))
        token_address = token_data_entry[1]

        with _open_or_create_file(token_data_entry) as csvfile:
            csv_reader = csv.reader(csvfile)
            _check_valid_csv(csv_reader)
            last_timestamp = _get_last_timestamp(csv_reader)
            price_data = _download_price_history(token_address, last_timestamp)
            
            csv_writer = csv.writer(csvfile)
            for data_point in price_data:
                timestamp = data_point["unixTime"]
                dt_object = datetime.datetime.fromtimestamp(timestamp, pytz.UTC)
                value = data_point["value"]
                csv_writer.writerow((timestamp, dt_object.strftime("%Y-%m-%d %H:%M:%S %Z"), value))