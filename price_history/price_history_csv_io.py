import csv
import datetime
import os
import pandas as pd
import pytz
import requests
import sys

from io import TextIOWrapper
from typing import Any, Dict, Tuple, List

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
# Now you can import parent_module

# Use functions or classes from parent_module
from secret import ALPHA_VANTAGE_API_KEY, BIRDEYE_API_KEY

# Ticker, Address, Public name
# When adding a new token, you can look up this data on birdeye.so:
# Ticker: what's written in the trading pair, e.g.: RENDER/USD => the ticker is "RENDER"
# Address: written on the left panel on birdeye.so
# Public name: written on the left panel on birdeye.so
_TOKEN_DATA = [
#   (ticker,                token_address,                  public_name)
    ("Mail", "C8cNX2D1y3jqKpMFkQhP1gGbfvTEdeckZXLBKSN5z5KF", "SolMail"),
    ("POPCAT", "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "POPCAT"),
    ("SOLC", "DLUNTKRQt7CrpqSX1naHUYoBznJ9pvMP65uCeWQgYnRK", "SolCard"),
    ("WHALES", "GTH3wG3NErjwcf7VGCoXEXkgXSHvYhx5gtATeeM5JAS1", "Whales Market"),
    ("RENDER", "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof", "Render Token"),
    ("JLP", "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4", "Jupiter Perps LP"),
    ("TRUMP", "HaP8r3ksG76PhQLTqR8FYBeNiQpejcFbQmiHbg787Ut1", "MAGA (Wormhole)"),
    ("coby", "8WnQQRbuEZ3CCDbH5MCVioBbw6o75NKANq9WdPhBDsWo", "coby"),
    ("MOODENG", "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY", "Moo Deng"),
    ("FWOG", "A8C3xuqscfmyLrte3VmTqrAq8kgMASius9AFNANwpump", "FWOG"),
    ("BAKED", "CQbXk942c6GPcRwtZ2WMFP5JcQ9NqbXtb8jUewBi7GoT", "BAKED"),
    ("DEGOD", "degod39zqQWzpG6h4b7SJLLTCFE6FeZnZD8BwHBFxaN", "DEGOD"),
    ("PUPS", "PUPS8ZgJ5po4UmNDfqtDMCPP6M1KP3EEzG9Zufcwzrg", "Migrate Your Pups"),
    ("MEW", "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5", "cat in a dogs world"),
    ("SIGMA", "5SVG3T9CNQsm2kEwzbRq6hASqh1oGfjqTtLXYUibpump", "SIGMA"),
    ("GUMMY", "H7bTHGb5Cvo5fGe5jBDNDPUv8KykQnzyZA3qZ8sH7yxw", "GUMMY"),
    ("GOAT", "CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump", "Goatseus Maximus"),
    ("XBG", "XBGdqJ9P175hCC1LangCEyXWNeCPHaKWA17tymz2PrY", "XBorg"),
    ("CLOUD", "CLoUDKc4Ane7HeQcPpE3YHnznRxhMimJ4MyaUqyHFzAu", "Cloud"),
    ("ACT", "GJAFwWjJ3vnTsrQVabjBVK2TYB1YtRCQXRDfDgUnpump", "Act I : The AI Prophecy"),
    ("LUCE", "CBdCxKo9QavR9hfShgpEBG3zekorAeD7W1jfq2o3pump", "Official Mascot of the Holy Year"),
    ("DIABOLO", "EoFrf9ZDRkzLsxX45JXZNDgibEsEdG8bFmaTnJpMpump", "Official Mascot Church Of Satan"),
    ("Pnut", "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump", "Peanut the Squirrel"),
    ("ME", "4Y47LEufvcSSSbTFojcvW4Y6x2KZXrqG2urNBSvHpump", "Literally Me"),
    ("NVIDIA", "47ixSEoXn9KUnZ9FjCD78NSWDd4RxKUjcwSSYMSnpump", "Nvidia On Solana"),
    ("SOLARIS", "3RfQPYVJgJbwyB3BzqqypCEWWryxjTfFDAcXQsckpump", "Solaris AI"),
    ("TITS", "23CTZMjEYNNZUE4itfn3iv6kgM4xn7X7dx1kVX1Gr8Xi", "We Love Tits"),
    ("EFR", "9So37icWcKmuMGFPPR36a82Q1E5gZJ6HFSz7Eynfpump", "End Federal Reserve"),
]

_DUST_TOKEN_DATA = [
    ("CHEEPEPE", "FwBixtdcmxawRFzBNeUmzhQzaFuvv6czs5wCQuLgWWsg", "cheepepe"),
    ("michi", "5mbK36SZ7J19An8jFochhQS4of8g6BwUjbeCSxBSoWdp", "michi"),
    ("TARDAR", "4CfSssjL3opCn5crmQexyMXoRJuL1BWd1JKAcgBmUYcB", "Grumpy Cat"),
    ("REXHAT", "AcV2T3mwLUqMiiqcsafVm35zwPQkmLrfRtaW3716Fzvi", "rexwifhat"),
    ("MARU", "DFVa5f8FtnwAimjL9NhqT8V1XZWxTQm8LomTcXERqPoi", "Maru Cat"),
    ("3LIONS", "3Ttb4uCyvAT4xYPRLWz3Vx7pwW5AdCdaU6E4FiJ6huB5", "Three Lions"),
    ("SOLCAT", "E99fN4tCRb1tQphXK1DU7prXji6hMzxETyPNJro19Fwz", "CatSolHat"),
    ("PAMP", "Ec5rX5Ctca6hA3sh6EBJWGH8fESk5F1HnypXNZ6zYaRw", "Pamp Cat"),
    ("FANA", "C31HuoXZr47Uy1HiTLhmso2PVfsop6thE45oAEsE1kE2", "Foolana"),
    ("POCAT", "DuhSwRVN7z8bWjYzwtRv2uDfpAsbTPxDnDezzi9Nsf1y", "Polite Cat"),
    ("BAREBEARS", "AeNg6DaCAjNpK7CvkSC6c9j5g8YFSp78aTQxejaNRNcz", "BAREBEARS"),
    ("BLAKCAT", "EEkZmJ9QtqELiy7Ybm2cNT5iWkZzog59SB6oXo5VdS4e", "BLAKCAT"),
    ("UNDQ", "GrGBy1gBHg6WiRUWqDaapjoP43bT2YdgDfJYuiCe8TDF", "UNDIQUE"),
    ("CatWifDog", "BET9FD4fpAz1BuFyMwGmLBQMCy1sTPn4hn8k3FvaL23i", "CatWifDog"),
    ("SHROOM", "xyzR4s6H724bUq6q7MTqWxUnhi8LM5fiKKUq38h8M1P", "Shroom"),
    ("TRUPE", "C2DbRWaoDJKMgegEKQuYCnViM9VdhrVR6FptPLm6stY7", "Trump PePe"),
    ("SOFAC", "9cC36bMZCt9fF53eLBQCago1czw5L9GRHZBL5adWEyAu", "SofaCat"),
    ("REL", "9HvM3nS2eDKWxNvi8vSFXRhHA1xhDW33rt4ZA1bE1DbV", "Relend"),
    ("LAPUPU", "8PH1XWXyhvFWXWeixSbcWmfHX4uRJHQtcjnwkCgzfTkY", "Lapupu"),
]

_TICKER_TO_DATAFRAME: Dict[str, pd.DataFrame] = {}
_usd_nok_dataframe = pd.DataFrame()
_token_data: List[Tuple[str, str, str]] = []
COL_NAME_TICKER = "Ticker"
COL_NAME_ADDRESS = "Address"
COL_NAME_PUBLIC_NAME = "Public name"

COL_NAME_TIMESTAMP = "Timestamp"
COL_NAME_TIME = "Time"
COL_NAME_PRICE_IN_USD = "Price in USD"

COL_NAME_DATE = "Date"
COL_NAME_OPEN = "Open"
COL_NAME_HIGH   = "High"
COL_NAME_LOW = "Low"
COL_NAME_CLOSE = "Close"

def get_token_data() -> List[Tuple[str,str,str]]:
    global _token_data
    if 0 == len(_token_data):
        _token_data = _TOKEN_DATA + _DUST_TOKEN_DATA
    return _token_data

def get_ticker_by_token_address(token_address: str) -> str:
    for token_data in get_token_data():
        if token_data[1] == token_address:
            return token_data[0]
        
    raise RuntimeError(token_address + " not found. Add token data manually in price_history_csv_io.py")

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
    for token_data in get_token_data():
        ticker = token_data[0]
        file_name = "price_history/data/" + ticker + ".csv"
        df = pd.read_csv(file_name, skiprows=2, parse_dates=[COL_NAME_TIME])
        df.set_index(COL_NAME_TIMESTAMP, inplace=True)
        _TICKER_TO_DATAFRAME[ticker] = df

def update_price_history() -> None:
    for token_data_entry in get_token_data():
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

def download_usd_nok_price_history() -> None:

    url = ("https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=NOK&outputsize=full&apikey=" 
        + ALPHA_VANTAGE_API_KEY)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Could not download USDNOK price history. Statis code {response.status_code}")

    json_data = response.json()  # Can throw requests.exceptions.JSONDecodeError. Lets the caller handle it
    if 0 == len(json_data["Time Series FX (Daily)"].items()):
        raise Exception("Got empty USDNOK data.")
    
    with open("price_history/data/USDNOK.csv", 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow((COL_NAME_DATE, COL_NAME_OPEN, COL_NAME_HIGH, COL_NAME_LOW, COL_NAME_CLOSE))

        # we need to reverse it, otherwise the latest data comes first 
        reversed_items = reversed(list(json_data["Time Series FX (Daily)"].items()))

        for date, ohlcv in reversed_items:
            csv_writer.writerow((date, ohlcv["1. open"], ohlcv["2. high"], ohlcv["3. low"], ohlcv["4. close"]))

def _load_usd_nok_dataframe() -> None:
    global _usd_nok_dataframe
    file_name = "price_history/data/USDNOK.csv"
    df = pd.read_csv(file_name, parse_dates=[COL_NAME_DATE])
    df = df.assign(Timestamp = df[COL_NAME_DATE].apply(lambda x: x.timestamp()))
    df.set_index("Timestamp", inplace=True)
    _usd_nok_dataframe = df
    # print(_usd_nok_dataframe.dtypes)

def get_usd_nok_dataframe() -> pd.DataFrame:
    if _usd_nok_dataframe.empty:
        _load_usd_nok_dataframe()
    return _usd_nok_dataframe