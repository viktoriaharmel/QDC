import pandas as pd

from utils import transform_date
from datetime import time


event = 'FTX'

# Prepare additional data on macroeconomic indicators
five_year_inflation = pd.read_csv('additional_data/5_Year_Breakeven_Inflation_Rate.csv', delimiter=';')
five_year_inflation['observation_date'] = five_year_inflation['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
five_year_inflation['T5YIE'] = pd.to_numeric(five_year_inflation['T5YIE'].str.replace(',', '.'))
five_year_inflation = five_year_inflation.rename(columns={'observation_date': 'time'})

ten_year_inflation = pd.read_csv('additional_data/10_Year_Breakeven_Inflation_Rate.csv', delimiter=';')
ten_year_inflation['observation_date'] = ten_year_inflation['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
ten_year_inflation['T10YIE'] = pd.to_numeric(ten_year_inflation['T10YIE'].str.replace(',', '.'))
ten_year_inflation = ten_year_inflation.rename(columns={'observation_date': 'time'})

market_yield_2_year_treasury = pd.read_csv('additional_data/Market_Yield_on_U.S._Treasury_Securities_at_2_Year.csv', delimiter=';')
market_yield_2_year_treasury['observation_date'] = market_yield_2_year_treasury['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
market_yield_2_year_treasury['DGS2'] = pd.to_numeric(market_yield_2_year_treasury['DGS2'].str.replace(',', '.'))
market_yield_2_year_treasury = market_yield_2_year_treasury.rename(columns={'observation_date': 'time'})

market_yield_5_year_treasury = pd.read_csv('additional_data/Market_Yield_on_U.S._Treasury_Securities_at_5_Year.csv', delimiter=';')
market_yield_5_year_treasury['observation_date'] = market_yield_5_year_treasury['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
market_yield_5_year_treasury['DGS5'] = pd.to_numeric(market_yield_5_year_treasury['DGS5'].str.replace(',', '.'))
market_yield_5_year_treasury = market_yield_5_year_treasury.rename(columns={'observation_date': 'time'})

market_yield_10_year_treasury = pd.read_csv('additional_data/Market_Yield_on_U.S._Treasury_Securities_at_10_Year.csv', delimiter=';')
market_yield_10_year_treasury['observation_date'] = market_yield_10_year_treasury['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
market_yield_10_year_treasury['DGS10'] = pd.to_numeric(market_yield_10_year_treasury['DGS10'].str.replace(',', '.'))
market_yield_10_year_treasury = market_yield_10_year_treasury.rename(columns={'observation_date': 'time'})

market_yield_30_year_treasury = pd.read_csv('additional_data/Market_Yield_on_U.S._Treasury_Securities_at_30_Year.csv', delimiter=';')
market_yield_30_year_treasury['observation_date'] = market_yield_30_year_treasury['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
market_yield_30_year_treasury['DGS30'] = pd.to_numeric(market_yield_30_year_treasury['DGS30'].str.replace(',', '.'))
market_yield_30_year_treasury = market_yield_30_year_treasury.rename(columns={'observation_date': 'time'})

vix = pd.read_csv('additional_data/Volatility_Index_VIX.csv', delimiter=';')
vix['observation_date'] = vix['observation_date'].apply(lambda x: transform_date(x, time(12, 0, 0)))
vix['VIXCLS'] = pd.to_numeric(vix['VIXCLS'].str.replace(',', '.'))
vix = vix.rename(columns={'observation_date': 'time'})

sp_500_etf_trust = pd.read_csv('additional_data/SPDR_S&P_500_ETF_Trust.csv', delimiter=';')
sp_500_etf_trust = sp_500_etf_trust.rename(columns={'Date': 'time', 'Open': 'sp_open', 'Close': 'sp_close', 'High': 'sp_high', 'Low': 'sp_low', 'Volume': 'sp_volume'})
# Convert the string column to datetime
sp_500_etf_trust['time'] = pd.to_datetime(sp_500_etf_trust['time'])
# Format the datetime column as a string in the desired format
sp_500_etf_trust['time'] = sp_500_etf_trust['time'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
for col in ['sp_open', 'sp_close', 'sp_high', 'sp_low']:
    sp_500_etf_trust[col] = pd.to_numeric(sp_500_etf_trust[col].str.replace(',', '.'))

cmi_initial = pd.read_csv('additional_data/Cryptocurrency_Market_Index_(CMI).csv', delimiter=';')
cmi_initial = cmi_initial.drop(columns='Market Cap')

cmi_cols = ['time', 'cmi_value', 'cmi_volume']
cmi = pd.DataFrame(columns=cmi_cols)

for index, row in cmi_initial.iterrows():
    time1 = transform_date(row['Start'], time(0, 0, 0))
    time2 = transform_date(row['Start'], time(8, 0, 0))
    time3 = transform_date(row['Start'], time(16, 0, 0))
    time4 = transform_date(row['Start'], time(23, 0, 0))
    new_rows = list()
    new_rows.append({'time': time1, 'cmi_value': row['Open'], 'cmi_volume': row['Volume']})
    if abs(row['Open'] - row['High']) < abs(row['Open'] - row['Low']):
        new_rows.append({'time': time2, 'cmi_value': row['High'], 'cmi_volume': row['Volume']})
        new_rows.append({'time': time3, 'cmi_value': row['Low'], 'cmi_volume': row['Volume']})
    else:
        new_rows.append({'time': time2, 'cmi_value': row['Low'], 'cmi_volume': row['Volume']})
        new_rows.append({'time': time3, 'cmi_value': row['High'], 'cmi_volume': row['Volume']})
    new_rows.append({'time': time4, 'cmi_value': row['Open'], 'cmi_volume': row['Volume']})
    new_df_rows = [pd.DataFrame([row]) for row in new_rows]
    # Append the new rows to the original DataFrame
    cmi = pd.concat([cmi] + new_df_rows, ignore_index=True)

orderbooks_df = pd.read_csv(f'{event}_data_enriched/orderbooks.csv')
trades_df = pd.read_csv(f'{event}_data_enriched/trades.csv')
quotes_df = pd.read_csv(f'{event}_data_enriched/quotes.csv')

# Assuming 'time', 'exchange', and 'symbol' are common identifiers
combined_df = pd.merge(orderbooks_df, trades_df, on=['time', 'exchange', 'symbol'], how='outer')
combined_df = pd.merge(combined_df, quotes_df, on=['time', 'exchange', 'symbol'], how='outer')
combined_df = pd.merge(combined_df, five_year_inflation, on=['time'], how='outer')
combined_df = pd.merge(combined_df, ten_year_inflation, on=['time'], how='outer')
combined_df = pd.merge(combined_df, market_yield_2_year_treasury, on=['time'], how='outer')
combined_df = pd.merge(combined_df, market_yield_5_year_treasury, on=['time'], how='outer')
combined_df = pd.merge(combined_df, market_yield_10_year_treasury, on=['time'], how='outer')
combined_df = pd.merge(combined_df, market_yield_30_year_treasury, on=['time'], how='outer')
combined_df = pd.merge(combined_df, vix, on=['time'], how='outer')
combined_df = pd.merge(combined_df, sp_500_etf_trust, on=['time'], how='outer')
combined_df = pd.merge(combined_df, cmi, on=['time'], how='outer')

# Alternatively, you can use concat for stacking vertically
# combined_df = pd.concat([orderbooks_df, trades_df, quotes_df], axis=1, join='outer')

# Create a dictionary of DataFrames, where each key is a unique market
markets_list = combined_df[['market', 'market_x', 'market_y']].stack().drop_duplicates().tolist()

# Initialize an empty dictionary to store datasets for each market
market_datasets = {}

# Iterate through the markets list
for market_value in markets_list:
    market_key = market_value  # Assuming 'market' is the first column in the markets list
    # Filter the combined_df based on the current market value in 'market', 'market_x', or 'market_y'
    market_data = combined_df[(combined_df['market'] == market_key) |
                              (combined_df['market_x'] == market_key) |
                              (combined_df['market_y'] == market_key)]
    #market_data = market_data.dropna(axis=1, how='all')
    market_data.sort_values(by='time', ascending=True, inplace=True)
    market_data['timestamp'] = range(1, len(market_data) + 1)
    market_data['id'] = range(1, len(market_data) + 1)
    market_data.set_index('id', inplace=True)
    market_data['market_indicator'] = market_data['market'].combine_first(market_data['market_x']).combine_first(market_data['market_y'])
    columns_to_drop = ['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0', 'market', 'market_x', 'market_y']
    market_data.drop(columns=columns_to_drop, inplace=True)
    market_data.rename(columns={'market_indicator': 'market'}, inplace=True)
    #market_data.reset_index(drop=True, inplace=True)    # reset index after matching timeframes

    # Add the filtered dataset to the dictionary with market_key as the key
    market_datasets[market_key] = market_data
#market_datasets = dict(tuple(combined_df.groupby('market')))

for market in market_datasets.keys():
    market_datasets[market].to_csv(f'{event}_data_combined_by_market/{market.replace("/", "_")}_combined_data.csv', index=False)

BTC_USD = market_datasets['ftx/BTC/USD']#pd.read_csv(f'{event}_data_combined_by_market/ftx_BTC_USD.csv')
print(BTC_USD.index)
print(BTC_USD)

