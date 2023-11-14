import re
from utils import combine_data_by_category, separate_data, load_data

event = 'FTX'

data_folder = f'{event}_data_cleaned'

data, filenames = load_data(data_folder)

trade_data, ob_data, quote_data = separate_data(data, filenames, True if event == 'FTX' else False)

def extract_market(file_name):
    # Define a regular expression pattern to match the trading pair
    pattern = r'([a-zA-Z0-9-]+)(_[a-zA-Z0-9-]+)'

    # Use re.match to search for the pattern in the file name
    match = re.match(pattern, file_name)

    # If a match is found, return the trading pair, otherwise return None
    return f'{match.group(1)}/{match.group(2)[1:]}' if match else None

# Example usage:
file_name1 = 'ftx_BTC-PERP_2022-11_quotes.csv'
file_name2 = 'okex_SOL-USDT_2022-11_orderbooks.csv'

trading_pair1 = extract_market(file_name1)
trading_pair2 = extract_market(file_name2)

print(trading_pair1)  # Output: binance-futures/BTCUSDT
print(trading_pair2)  # Output: gate-io/BTC_USDT

combined_data = combine_data_by_category(trade_data)

print(combined_data)