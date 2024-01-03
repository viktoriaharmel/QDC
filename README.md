# QDC

So far, the code covers the data preparation and exploration and can be executed on all provided datasets.

Execution order:

1. Some preliminary checks and first looks into the data are performed in the Script first_check.py
2. Reports on data quality and descriptive statistics as well as visualizations on the different data files are created in preparation.py
3. Data Cleansing can be performed in data_cleansing.py and the cleaned files are stored in <event>_data_cleaned
4. Feature Engineering for trades, order books, and quotes is conducted in feature_engineering.py and the results are stored in <event>_data_enriched
5. The enriched data serves as input to the data exploration script data_exploration.py. The outputs are stored in the <event>_exploration_results/visualizations folders
