import os
from utils import load_data

# Drop duplicates and NaNs

# Enter the Event for which the data should be prepared (FTX, FOMC_22, FOMC_23)
event = 'FTX'

data_folder = f'{event}_data'

data, filenames = load_data(data_folder)

for i in range(len(data)):
    file_name = os.path.basename(filenames[i])
    clean_data = data[i].dropna()
    clean_data = clean_data.drop_duplicates()
    clean_data.to_csv(path_or_buf=f'{event}_data_cleaned/{file_name}', index=False)