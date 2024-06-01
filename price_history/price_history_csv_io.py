import csv
import datetime
import os
import pytz
import requests
import sys

from io import TextIOWrapper
from typing import Any, Tuple

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
# Now you can import parent_module

# Use functions or classes from parent_module
from confidential import BIRDEYE_API_KEY

TOKEN_DATA = [
    ("Mail", "C8cNX2D1y3jqKpMFkQhP1gGbfvTEdeckZXLBKSN5z5KF", "SolMail"),
    ("POPCAT", "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "POPCAT"),
    ("SOLC", "DLUNTKRQt7CrpqSX1naHUYoBznJ9pvMP65uCeWQgYnRK", "SolCard"),
    ("WHALES", "GTH3wG3NErjwcf7VGCoXEXkgXSHvYhx5gtATeeM5JAS1", "Whales Market")
]

def open_or_create_file(token_data_entry: Tuple[str, str, str]) -> TextIOWrapper:
    ticker = token_data_entry[0]
    file_name = "price_history/data/" + ticker + ".csv"
    
    if not os.path.exists(file_name):
        with open(file_name, 'w+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(("Ticker","Address","Public name"))
            csv_writer.writerow(token_data_entry)
            csv_writer.writerow(("Timestamp","Time","Price in USD"))
    
    return open(file_name, 'r+', newline='')

def check_valid_csv(csv_reader) -> None:
    error_msg = "Invalid price history csv."
    
    line = next(csv_reader)
    if line[0] != "Ticker" or line[1] != "Address" or line[2] != "Public name":
        raise Exception(error_msg)
    
    line = next(csv_reader)
    if line[0] == "" or line[1] == "" or line[2] == "":
        raise Exception(error_msg)
    
    line = next(csv_reader)
    if line[0] != "Timestamp" or line[1] != "Time" or line[2] != "Price in USD":
        raise Exception(error_msg)

def get_last_timestamp(csv_reader) -> int:
    timestamp_str = ""
    for line in csv_reader:
        timestamp_str = line[0]

    try:
        return int(timestamp_str)
    except ValueError:
        return 0

def download_price_history(token_address: str, time_from: int) -> Any:
    time_from += 1
    time_to = int(datetime.datetime.now().timestamp())
    url = ("https://public-api.birdeye.so/defi/history_price?address=" + token_address + 
           "&address_type=token&type=4H&time_from=" + str(time_from) + 
           "&time_to=" + str(time_to))
    
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    
    response = requests.get(url, headers=headers)

    data = response.json()  # Can throw requests.exceptions.JSONDecodeError. Lets the caller handle it
    return data["data"]["items"]

def update_price_history() -> None:
    for token_data_entry in TOKEN_DATA:
        token_address = token_data_entry[1]

        with open_or_create_file(token_data_entry) as csvfile:
            csv_reader = csv.reader(csvfile)
            last_timestamp = get_last_timestamp(csv_reader)
            price_data = download_price_history(token_address, last_timestamp)
            
            csv_writer = csv.writer(csvfile)
            for data_point in price_data:
                timestamp = data_point["unixTime"]
                dt_object = datetime.datetime.fromtimestamp(timestamp, pytz.UTC)
                value = data_point["value"]
                csv_writer.writerow((timestamp, dt_object.strftime("%Y-%m-%d %H:%M:%S %Z"), value))

        
update_price_history()