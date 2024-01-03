import os
import re
from datetime import datetime
from datetime import time

import numpy as np
import pandas as pd


def load_data(data_folder: str):

    # Get a list of CSV files in the folder
    data_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.csv')]

    data_dfs = []
    filenames = []
    for data_file in data_files:
        data = pd.read_csv(data_file)
        file_name = os.path.basename(data_file)
        data_dfs.append(data)
        filenames.append(file_name)

    return data_dfs, filenames

def separate_data(data: list, filenames: list, with_quotes: bool):

    trade_data = []
    ob_data = []
    quote_data = []

    for i in range(len(data)):
        if 'trades' in filenames[i]:
            trade_data.append(data[i])
        elif 'orderbooks' in filenames[i]:
            ob_data.append(data[i])
        elif with_quotes:
            quote_data.append(data[i])

    return trade_data, ob_data, quote_data


def combine_data_by_category(data: list):

    columns = data[0].columns.tolist()
    columns.append('market')
    combined_data = pd.DataFrame(columns=columns)
    for df in data:
        df['market'] = df.apply(lambda row: f'{row["exchange"]}/{row["symbol"]}', axis=1)
        combined_data = pd.concat([combined_data, df], ignore_index=True)

    return combined_data


def extract_market(file_name):
    # Define a regular expression pattern to match the trading pair
    pattern = r'([a-zA-Z0-9-]+)(_[a-zA-Z0-9-]+)'

    # Use re.match to search for the pattern in the file name
    match = re.match(pattern, file_name)

    # If a match is found, return the trading pair, otherwise return None
    return f'{match.group(1)}/{match.group(2)[1:]}' if match else None


def crosscorr(df: pd.DataFrame, first: str, second: str) -> pd.DataFrame:
    """
    Helper Function to perform a Crosscorrelation analysis.

    :param df: (pl.DataFrame)
    :param first: (str) Name of the first asset
    :param second: (str) Name of the second asset
    """
    shift_range = range(-20, 21)
    correlation_results = []

    for shift in shift_range:
        df_shifted = df.assign(**{f"value_shifted_{shift}": df[first].shift(shift)})
        joined_df = df_shifted[[second, f"value_shifted_{shift}"]].dropna()
        correlation = joined_df.corr().loc[second, f"value_shifted_{shift}"]
        correlation_results.append({"Shift": shift, "Correlation": correlation})

    correlation_df = pd.DataFrame(correlation_results)

    return correlation_df


def get_trades_pivot(trades: pd.DataFrame, base: str, event: str, value: str):

    base_trades = trades[trades['base'] == base]
    trades_pivot = pd.pivot_table(base_trades, values=value, index='time', columns='market',
                                  aggfunc='last').sort_values(by='time')
    trades_pivot = trades_pivot.fillna(method='ffill', axis=1)

    trades_pivot = trades_pivot.apply(lambda col: col.apply(np.log)).diff()
    trades_pivot.to_csv(f'{event}_exploration_results/{value}_per_market_{base}.csv')
    # trades_pivot.with_columns(pl.all().exclude('time').diff())

    return trades_pivot


# Function to convert a date string to the desired format
def transform_date(input_date_str, reference_time):
    input_date = datetime.strptime(input_date_str, '%Y-%m-%d')
    result_datetime = datetime.combine(input_date.date(), reference_time)
    result_str = result_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return result_str
