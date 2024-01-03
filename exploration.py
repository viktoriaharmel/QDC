import os

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from utils import load_data, crosscorr, get_trades_pivot

# Call the defined functions with the desired parameters to perform a customized explorative analysis


# Enter the Event for which the data should be prepared (FTX, FOMC_22, FOMC_23)
event = 'FTX'
excel_report = f'{event}_exploration_results.xlsx'

trades = pd.read_csv(f'{event}_data_enriched/trades.csv')
orderbooks = pd.read_csv(f'{event}_data_enriched/orderbooks.csv')
quotes = pd.read_csv(f'{event}_data_enriched/quotes.csv')

#orderbooks = orderbooks.drop('timestamp', axis=1)

trades['time'] = pd.to_datetime(trades['time'])     # to feature engineering
orderbooks['time'] = pd.to_datetime(orderbooks['time'])
quotes['time'] = pd.to_datetime(quotes['time'])

trades['base'] = trades['symbol'].str.slice(0, 3)

# Get unique values of base currencies
base_currencies = trades['base'].unique().tolist()

markets = trades['market'].unique().tolist()

exchanges = trades['exchange'].unique().tolist()


# target feature developments per currency and exchange

def get_feature_development_per_currency(base_currencies: list, trades: pd.DataFrame, exchanges: list, feature: str):

    fig, axs = plt.subplots(len(base_currencies), figsize=(15, 14))

    for i in range(len(base_currencies)):
        axs[i].set_title(base_currencies[i])
        tmp = trades[trades['base'] == base_currencies[i]]
        common_time_frame = tmp.groupby('exchange')['time'].agg(['max', 'min']).max(axis=1)
        tmp = tmp[tmp['time'].between(common_time_frame.min(), common_time_frame.max())]
        for exchange in exchanges:
            tmp1 = tmp[tmp['exchange'] == exchange]
            axs[i].plot(tmp1['time'], tmp1[feature], label=exchange)

        axs[i].legend()

    plt.show()
    plot_filename = f'{feature}_development_per_base_currency'
    plt.savefig(f'{event}_exploration_visualizations/' + plot_filename)
    plt.close()


get_feature_development_per_currency(base_currencies, trades, exchanges, 'vwap')


# Trade volume per market

def get_trade_volume_per_market(trades: pd.DataFrame):

    # Group by 'market' and aggregate sum of 'volume' and 'number_trades'
    grpd_trades = trades.groupby('market').agg({'volume': 'sum', 'number_trades': 'sum'})
    grpd_trades['volume share'] = grpd_trades['volume'] / sum(grpd_trades['volume'])
    grpd_trades = grpd_trades.reset_index()
    # Sort by 'volume' in descending order
    grpd_trades = grpd_trades.sort_values(by='volume', ascending=False)

    with pd.ExcelWriter(excel_report, engine='xlsxwriter') as writer:
        grpd_trades.to_excel(writer, sheet_name='trades per market', index=False)


get_trade_volume_per_market(trades)


# Market relationships regarding average price change

def get_feature_pct_change_market_comparison(orderbooks: pd.DataFrame, feature: str ,markets=None):

    if not markets:
        markets = orderbooks['market'].unique().tolist()
    n = len(markets)
    fig, axs = plt.subplots(n, n, figsize=(45, 45))

    plt.subplots_adjust(wspace=0, hspace=0)

    for i in range(n):
        for j in range(n):

            common_time_frame = \
                orderbooks[orderbooks['market'].isin([markets[i], markets[j]])].groupby('market')[
                    'timestamp'].agg(['max', 'min'])#.max(axis=1)
            common_time_frame.loc[:, 'min'] = common_time_frame['min'].max()
            common_time_frame.loc[:, 'max'] = common_time_frame['max'].min()
            ax = axs[i, j]
            tmp1 = orderbooks[orderbooks['market'] == markets[i]]
            tmp1 = tmp1[tmp1['timestamp'].between(common_time_frame.loc[markets[i], 'min'], common_time_frame.loc[markets[i], 'max'])]
            tmp1 = tmp1.groupby(['time', 'exchange', 'symbol', 'market'], as_index=False).agg({'top_bid': 'mean', 'top_ask': 'mean',
                'amount_bid_10': 'mean', 'amount_bid_25': 'mean', 'amount_ask_10': 'mean', 'amount_ask_25': 'mean',
                'spread': 'mean', 'total_bid_volume': 'mean', 'total_ask_volume': 'mean', 'volume_imbalance': 'mean',
                'average_price': 'mean'})

            if i == j:
                ax.hist(tmp1[feature].pct_change(), bins=25)
            else:
                tmp2 = orderbooks[orderbooks['market'] == markets[j]]
                tmp2 = tmp2[tmp2['timestamp'].between(common_time_frame.loc[markets[j], 'min'], common_time_frame.loc[markets[j], 'max'])]

                common_times = pd.merge(tmp1[['time']], tmp2[['time']], on='time', how='inner')

                # Filter original dataframes based on common times
                tmp1 = tmp1[tmp1['time'].isin(common_times['time'])]
                tmp2 = tmp2[tmp2['time'].isin(common_times['time'])]

                tmp2 = tmp2.groupby(['time', 'exchange', 'symbol', 'market'], as_index=False).agg({'top_bid': 'mean', 'top_ask': 'mean',
                     'amount_bid_10': 'mean', 'amount_bid_25': 'mean', 'amount_ask_10': 'mean', 'amount_ask_25': 'mean',
                     'spread': 'mean', 'total_bid_volume': 'mean', 'total_ask_volume': 'mean', 'volume_imbalance': 'mean',
                     'average_price': 'mean'})


                ax.scatter(tmp1[feature].pct_change(), tmp2[feature].pct_change(), s=1)

            ax.tick_params(axis='both', labelsize=4)

            if j != 0:
                ax.yaxis.set_visible(False)
            else:
                ax.set_ylabel(markets[i], fontsize=6)
            if i != n - 1:
                ax.xaxis.set_visible(False)
            else:
                ax.set_xlabel(markets[j], fontsize=6)

    plt.show()
    plot_filename = f'market_comparison_{feature}'
    plt.savefig(f'{event}_exploration_visualizations/' + plot_filename)
    plt.close()


get_feature_pct_change_market_comparison(orderbooks, 'average_price')


# Create trades pivot tables


def get_trades_pivot_table(base_currencies: list, trades: pd.DataFrame, pivot_value: str, markets=None):
    for base in base_currencies:

        trades_pivot = get_trades_pivot(trades, base, event, pivot_value)

        if markets is None:
            markets = trades_pivot.columns

        comp_stats = pd.DataFrame(columns=['market_comparison', 'statistic', 'value'])

        for i in range(len(markets)):
            for j in range(len(markets)):
                if i == j:
                    continue
                if markets[i] in ['binance-futures/BTCUSDT', 'binance/BTCUSDT'] or markets[j] in ['binance-futures/BTCUSDT', 'binance/BTCUSDT'] :
                    continue
                comparison = f'{markets[i]} - {markets[j]}'
                diff_description = (trades_pivot[markets[i]] - trades_pivot[markets[j]]).describe()

                rows_to_concat = pd.DataFrame({
                    'market_comparison': [comparison] * len(diff_description),
                    'statistic': diff_description.index,
                    'value': diff_description.values
                })

                comp_stats = pd.concat([comp_stats, rows_to_concat], ignore_index=True)

                plt.hist(trades_pivot[markets[i]] - trades_pivot[markets[j]], bins=100)
                plt.title(f'{event}_{pivot_value}_{markets[i]} - {markets[j]}')
                plt.axvline(0, color='red')
                plt.grid()
                plt.show()
                plt.savefig(f'{event}_exploration_visualizations/{pivot_value}_market_comparison_{markets[i][:markets[i].find("/")]}_{markets[j][:markets[j].find("/")]}_{base}')
                plt.close()

        comp_stats.to_csv(f'{event}_exploration_results/{pivot_value}_market_comparison_{base}.csv')


#get_trades_pivot_table(base_currencies, trades, 'vwap')


# Calculate cross correlations

def get_cross_correlations(base_currencies: list, trades: pd.DataFrame, pivot_value: str, markets=None):

    for base in base_currencies:

        trades_pivot = get_trades_pivot(trades, base, event, pivot_value)

        if markets is None:
            markets = trades_pivot.columns

        corrs = pd.DataFrame(columns=['market_comparison', 'Shift', 'Correlation'])

        for i in range(len(markets)):
            for j in range(i + 1, len(markets)):
                comparison = f'{markets[i]} - {markets[j]}'
                ll = crosscorr(trades_pivot, markets[i], markets[j])
                #ll = ll[ll['Correlation'] == ll['Correlation'].max()]

                rows_to_concat = pd.DataFrame({
                    'market_comparison': [comparison] * len(ll),
                    'Shift': ll['Shift'],
                    'Correlation': ll['Correlation']
                })

                corrs = pd.concat([corrs, rows_to_concat], ignore_index=True)

                fig, ax = plt.subplots(figsize=(15, 7), facecolor='white')

                ax.plot(ll['Shift'], ll['Correlation'])
                ax.set_ylabel('Lead-Lag in ms')
                ax.set_title(f"{markets[i]} vs. {markets[j]}")
                ax.grid()
                plt.show()
                plt.savefig(f'{event}_exploration_visualizations/lead_lag_ms_{markets[i][:markets[i].find("/")]}_{markets[j][:markets[j].find("/")]}_{base}')
                plt.close()
        corrs.to_csv(f'{event}_exploration_results/Cross_Correlations_{base}.csv')


#get_cross_correlations(base_currencies, trades, 'vwap')

# Measure liquidity

def get_amihuds(trades: pd.DataFrame, markets=None):

    if markets is None:
        markets = trades['market'].unique().tolist()

    amihuds = {}
    amihuds_agg = []

    # Iterate through markets
    for market in markets:
        # Prepare a tmp dataframe from the market and take the pct_changes of the VWAP.
        tmp = trades[trades['market'] == market].sort_values('time')
        tmp['returns'] = tmp['vwap'].pct_change()
        tmp = tmp.dropna(subset=['returns'])

        # Calculate the Amihud Ratio and crop it so we can later join everything together
        amihuds[market] = (tmp['returns'].abs() / tmp['volume']).iloc[-1242:]
        amihuds_agg.append([market, (tmp['returns'].abs() / tmp['volume']).mean() * 10_000])

    # Convert dictionaries to DataFrames
    amihuds_df = pd.DataFrame(amihuds)
    print(amihuds_df)
    amihuds_agg_df = pd.DataFrame(amihuds_agg, columns=['market', 'amihud'])

    # Multiply amihuds by 10,000 and describe
    result_description = (amihuds_df * 10_000).describe()

    amihuds_comp = amihuds_agg_df.sort_values(by='amihud')
    print(amihuds_comp)

    plt.figure(figsize=(25, 6))

    # Create a single boxplot without specifying labels
    plt.boxplot(amihuds_df.values, vert=False)

    plt.title('Amihud Comparison')
    plt.xlabel('Amihud Ratio')
    plt.yscale('log')
    plt.xticks(range(1, len(amihuds_df.columns) + 1), amihuds_df.columns)
    plt.grid()

    plt.show()

    print(result_description)


get_amihuds(trades)

# Correlation Heatmap

def get_correlation_heatmap(data: str, numeric_columns=None):

    if data == 'trades':
        if numeric_columns is None:
            numeric_columns = trades.select_dtypes(include=['number']).columns

            # Filter out datetime columns
            numeric_columns = [col for col in numeric_columns if trades[col].dtype != 'datetime']

        # Create a new DataFrame with only numeric and non-datetime columns
        trades_tmp = trades[numeric_columns]

        trade_corr = trades_tmp.corr()

        plt.figure(figsize=(12, 10))
        sns.heatmap(trade_corr, annot=True, cmap='coolwarm', fmt=".2f")
        plt.title('Correlation Heatmap for trades')
        plot_filename = "trades_correlation_matrix.png"
        plt.savefig(f'{event}_exploration_visualizations/{plot_filename}')
        plt.show()

    if data == 'orderbooks':
        if numeric_columns is None:
            numeric_columns = trades.select_dtypes(include=['number']).columns

            # Filter out datetime columns
            numeric_columns = [col for col in numeric_columns if trades[col].dtype != 'datetime']

        # Create a new DataFrame with only numeric and non-datetime columns
        orderbooks_tmp = orderbooks[numeric_columns]

        ob_corr = orderbooks_tmp.corr()

        plt.figure(figsize=(12, 10))
        sns.heatmap(ob_corr, annot=True, cmap='coolwarm', fmt=".2f")
        plt.title('Correlation Heatmap for orderbooks')
        plot_filename = "orderbooks_correlation_matrix.png"
        plt.savefig(f'{event}_exploration_visualizations/{plot_filename}')
        plt.show()

    if data == 'quotes':
        if numeric_columns is None:
            numeric_columns = trades.select_dtypes(include=['number']).columns

            # Filter out datetime columns
            numeric_columns = [col for col in numeric_columns if trades[col].dtype != 'datetime']

        # Create a new DataFrame with only numeric and non-datetime columns
        quotes_tmp = quotes[numeric_columns]

        quotes_corr = quotes_tmp.corr()

        plt.figure(figsize=(12, 10))
        sns.heatmap(quotes_corr, annot=True, cmap='coolwarm', fmt=".2f")
        plt.title('Correlation Heatmap for quotes')
        plot_filename = "quotes_correlation_matrix.png"
        plt.savefig(f'{event}_exploration_visualizations/{plot_filename}')
        plt.show()


get_correlation_heatmap('trades')