import os
import pandas as pd
import sys

from price_history.price_history_csv_io import _TICKER_TO_DATAFRAME, _TOKEN_DATA, _load_csv_to_dataframes, COL_NAME_TIMESTAMP, COL_NAME_PRICE_IN_USD
from datetime import datetime

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add the parent directory to sys.path
sys.path.insert(0, parent_dir)
# Now you can import parent_module

# Use functions or classes from parent_module
from constants import DATE_FORMAT_FROM_STAKE_TAX

def is_price_history_locally_available(ticker: str) -> bool:
    for token_data in _TOKEN_DATA:
        if token_data[0] == ticker:
            return True
        
    return False

def price_at_time(ticker: str, date_time: str) -> float:
    if 0 == len(_TICKER_TO_DATAFRAME):
        _load_csv_to_dataframes()

    if ticker not in _TICKER_TO_DATAFRAME:
        raise Exception("ticker {} not found in the downloaded data.".format(ticker))
    
    timestamp = datetime.strptime(date_time, DATE_FORMAT_FROM_STAKE_TAX).timestamp()
    price_history_df = _TICKER_TO_DATAFRAME[ticker]

    pos = price_history_df.index.searchsorted(timestamp)

    # last timestamp 1719129600

    # Get the rows between which the given timestamp falls
    if pos == 0:
        # Timestamp is before the first row. Return the first known price
        return price_history_df.iloc[0][COL_NAME_PRICE_IN_USD]
    elif pos == len(price_history_df):
        # Timestamp is before the last row. Return the last known price
        return price_history_df.iloc[-1][COL_NAME_PRICE_IN_USD]

    # interpolate between the two prices
    ts_before = price_history_df.index[pos - 1]
    ts_after = price_history_df.index[pos]
    price_before = price_history_df.iloc[pos - 1][COL_NAME_PRICE_IN_USD]
    price_after = price_history_df.iloc[pos][COL_NAME_PRICE_IN_USD]
    return (price_before + ((price_after - price_before) * (timestamp - ts_before) / (ts_after - ts_before)))
