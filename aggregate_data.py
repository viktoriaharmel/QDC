import pandas as pd

from utils import transform_date
from datetime import time
from scipy import interpolate
import os

directory_path = '/Users/Viktoria_1/PycharmProjects/QDC/FTX_data_combined_by_market'

# Iterate over all files in the directory
for filename in os.listdir(directory_path):
    if filename == '.DS_Store':
        continue
    file_path = os.path.join(directory_path, filename)

    original_data = pd.read_csv(file_path)
    aggregated_data = original_data.copy()
    aggregated_data.drop(columns=['exchange', 'symbol', 'timestamp', 'market'], inplace=True)

    # Convert 'time' column to datetime object
    aggregated_data['time'] = pd.to_datetime(aggregated_data['time'])

    # Group by hourly intervals and calculate the mean
    aggregated_data = aggregated_data.groupby(aggregated_data['time'].dt.floor('H')).mean()
    aggregated_data.set_index('time', inplace=True)

    # Reset index to make 'time' a column again
    #aggregated_data.reset_index(inplace=True)

    aggregated_data['timestamp'] = range(1, len(aggregated_data) + 1)


    aggregated_data['symbol'] = [original_data['symbol'][0] for _ in range(len(aggregated_data))]
    aggregated_data['exchange'] = [original_data['exchange'][0] for _ in range(len(aggregated_data))]
    aggregated_data['market'] = [original_data['market'][0] for _ in range(len(aggregated_data))]

    data_filled = aggregated_data.interpolate(method='time')
    data_filled.reset_index(inplace=True)
    data_filled['id'] = range(1, len(data_filled) + 1)
    data_filled.set_index('id', inplace=True)

    market = original_data['market'][0]
    data_filled.to_csv(f'FTX_data_aggregated_filled/{market.replace("/", "_")}_data_filled.csv')





