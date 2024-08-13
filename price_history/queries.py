import os
import pandas as pd
import sys

from price_history.price_history_csv_io import (
    _TICKER_TO_DATAFRAME, 
    _load_csv_to_dataframes,
    COL_NAME_PRICE_IN_USD,
    get_usd_nok_dataframe,
    COL_NAME_CLOSE,
    get_token_data
)

from datetime import datetime

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
# Now you can import parent_module

# Use functions or classes from parent_module
from constants import DATE_FORMAT_FROM_STAKE_TAX

def is_price_history_locally_available(ticker: str) -> bool:
    for token_data in get_token_data():
        if token_data[0] == ticker:
            return True
        
    return False

def _interpolate_price(data_frame: pd.DataFrame, col_name: str, timestamp: datetime) -> float:
    
    pos = data_frame.index.searchsorted(timestamp)

    # Get the rows between which the given timestamp falls
    if pos == 0:
        # Timestamp is before the first row. Return the first known price
        return data_frame.iloc[0][col_name]
    elif pos == len(data_frame):
        # Timestamp is before the last row. Return the last known price
        return data_frame.iloc[-1][col_name]

    # interpolate between the two prices
    ts_before = data_frame.index[pos - 1]
    ts_after = data_frame.index[pos]
    price_before = data_frame.iloc[pos - 1][col_name]
    price_after = data_frame.iloc[pos][col_name]
    
    return (price_before + ((price_after - price_before) * (timestamp - ts_before) / (ts_after - ts_before)))

def price_at_time(ticker: str, date_time: str) -> float:
    if 0 == len(_TICKER_TO_DATAFRAME):
        _load_csv_to_dataframes()

    if ticker not in _TICKER_TO_DATAFRAME:
        raise Exception("ticker {} not found in the downloaded data.".format(ticker))
    
    timestamp = datetime.strptime(date_time, DATE_FORMAT_FROM_STAKE_TAX).timestamp()
    price_history_df = _TICKER_TO_DATAFRAME[ticker]

    return _interpolate_price(price_history_df, COL_NAME_PRICE_IN_USD, timestamp)

def convert_usd_to_nok(usd_value: float, date_time: str) -> float:
        
    timestamp = datetime.strptime(date_time, DATE_FORMAT_FROM_STAKE_TAX).timestamp()

    usd_nok_rate = _interpolate_price(get_usd_nok_dataframe(), COL_NAME_CLOSE, timestamp)
    return usd_value * usd_nok_rate