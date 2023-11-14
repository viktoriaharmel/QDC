import pandas as pd

ftx_binance_spot_btc_orderbooks_df = pd.read_csv('FTX_data/binance_BTCUSDT_2022-11_trades.csv')
print(ftx_binance_spot_btc_orderbooks_df.shape)
print(ftx_binance_spot_btc_orderbooks_df.head())
print(ftx_binance_spot_btc_orderbooks_df.columns)
print(ftx_binance_spot_btc_orderbooks_df.dtypes)

ftx_binance_spot_btc_orderbooks_isna = ftx_binance_spot_btc_orderbooks_df.isna()
print(ftx_binance_spot_btc_orderbooks_isna.head())
print(ftx_binance_spot_btc_orderbooks_isna.describe())
print(ftx_binance_spot_btc_orderbooks_df.duplicated())

def print_nan_count(series: pd.Series):
    nan_sum = series.isna().sum()
    print(f"Identified {nan_sum} duplicate values "
          f"({nan_sum/len(series)*100:02.2f}%) "
          f"in '{series.name}'"
         )

for column in ftx_binance_spot_btc_orderbooks_df.columns:
    print_nan_count(ftx_binance_spot_btc_orderbooks_df[column])

def summarize_na(df: pd.DataFrame) -> pd.DataFrame:
    nan_count = df.isna().sum()
    return pd.DataFrame({'nan_count': nan_count,
                         'nan_pct': nan_count / len(df) * 100
                         }
                        )[nan_count > 0]

ftx_binance_spot_btc_orderbooks_df_nan_sum = summarize_na(ftx_binance_spot_btc_orderbooks_df)
print(ftx_binance_spot_btc_orderbooks_df_nan_sum)

above_threshold = (ftx_binance_spot_btc_orderbooks_df_nan_sum['nan_pct'] >= 50)
print(ftx_binance_spot_btc_orderbooks_df_nan_sum[above_threshold].sort_values('nan_pct'))  # sort by ascending values
