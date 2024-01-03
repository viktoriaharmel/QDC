import os
import pandas as pd
from utils import load_data, separate_data, combine_data_by_category

# Enter the Event for which the data should be prepared (FTX, FOMC_22, FOMC_23)
event = 'FTX'

data_folder = f'{event}_data_cleaned'

data, filenames = load_data(data_folder)

# Split the data files for trades, order books and quotes
trade_data_distinct, ob_data_distinct, quote_data_distinct = separate_data(data, filenames, True if event == 'FTX' else False)

# Combines the files of each category into one file with column for the respective market
trade_data = combine_data_by_category(trade_data_distinct)
ob_data = combine_data_by_category(ob_data_distinct)
quote_data = combine_data_by_category(quote_data_distinct)

# Feature Engineering for Order Book Data
def create_order_book_features(order_book_data):
    # Calculate the bid-ask spread
    order_book_data['spread'] = order_book_data['top_ask'] - order_book_data['top_bid']

    # Calculate the total bid and ask volume
    order_book_data['total_bid_volume'] = order_book_data['amount_bid_10'] + order_book_data['amount_bid_25']
    order_book_data['total_ask_volume'] = order_book_data['amount_ask_10'] + order_book_data['amount_ask_25']

    # Calculate the bid-ask volume imbalance
    order_book_data['volume_imbalance'] = order_book_data['total_bid_volume'] - order_book_data['total_ask_volume']

    # Calculate the average bid-ask price
    order_book_data['average_price'] = (order_book_data['top_bid'] + order_book_data['top_ask']) / 2

    return order_book_data


# Feature Engineering for Trade Data
def create_trade_features(trade_data):
    # Calculate the price difference between trades
    trade_data['price_diff'] = trade_data['amount'] - trade_data['amount'].shift(1)

    # Calculate the rolling mean of trade prices
    trade_data['rolling_mean_price'] = trade_data['amount'].rolling(window=5).mean()

    # Calculate the rolling volume of trades
    trade_data['rolling_volume'] = trade_data['volume'].rolling(window=5).sum()

    trade_data['vwap'] = trade_data['volume'] / trade_data['amount']

    # Calculate the Amihud ratio
    #trade_data['amihud'] = abs(trade_data['price_diff']) / trade_data['volume']
    trade_data['amihud'] = abs(trade_data['vwap'].pct_change()) / trade_data['volume']

    return trade_data


# Feature Engineering for Quotes Data
def create_quote_features(quote_data):
    # Check if required columns are present
    # required_columns = ['amount', 'amount_buy', 'exchange', 'number_trades', 'number_trades_buy',
    #                     'symbol', 'time', 'volume', 'volume_buy', 'amount_ask_10', 'amount_ask_25',
    #                     'amount_bid_10', 'amount_bid_25', 'ask_amount', 'ask_price', 'bid_amount',
    #                     'bid_price', 'timestamp', 'top_ask', 'top_bid']
    #
    # missing_columns = set(required_columns) - set(quote_data.columns)
    #
    # for column in missing_columns:
    #     quote_data[column] = None

    #quote_data['average_trade_amount'] = quote_data['amount'] / quote_data['number_trades']
    quote_data['ask_bid_ratio'] = quote_data['ask_amount'] / quote_data['bid_amount']
    #quote_data['trade_volume_ratio'] = quote_data['volume'] / quote_data['volume_buy']
    #quote_data['ask_price_ratio'] = quote_data['ask_price'] / quote_data['top_ask']
    #quote_data['bid_price_ratio'] = quote_data['bid_price'] / quote_data['top_bid']

    # Calculate bid-ask price ratio
    #quote_data['bid_ask_price_ratio'] = quote_data['top_bid'] / quote_data['top_ask']

    # Calculate the percentage change in bid-ask prices
    #quote_data['price_change_percentage'] = quote_data['top_ask'].pct_change() * 100

    # Calculate the percentage change in bid-ask volumes
    #quote_data['volume_change_percentage'] = quote_data['volume'].pct_change() * 100

    return quote_data


# Create relevant features for order book data
order_book_data = create_order_book_features(ob_data)
order_book_data.to_csv(f'{event}_data_enriched/orderbooks.csv', index=False)

# Create relevant features for trade data
tr_data = create_trade_features(trade_data)
tr_data.to_csv(f'{event}_data_enriched/trades.csv', index=False)

# Create relevant features for quotes data
q_data = create_quote_features(quote_data)
q_data.to_csv(f'{event}_data_enriched/quotes.csv', index=False)

