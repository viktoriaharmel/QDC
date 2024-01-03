import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Enter the Event for which the data should be prepared (FTX, FOMC_22, FOMC_23)
event = 'FOMC_23'

# Function to create a data report
def create_data_report(data, file_name):
    # DataFrame to store the report
    report_df = pd.DataFrame(columns=['File Name', 'Column Name', 'NaN Count', 'NaN Percentage', 'Data Types', 'Duplicate Rows'])

    # Check for duplicate rows across the entire dataset
    duplicate_rows = data.duplicated().sum()

    numeric_columns = data.select_dtypes(include='number')

    # Loop through each column in the data
    for column in data.columns:
        nan_count = data[column].isna().sum()
        nan_percentage = (nan_count / len(data)) * 100
        data_type = data[column].dtype

        # Detect and count outliers using the Z-score method
        # if column in numeric_columns.columns:
        #     z_scores = np.abs((data[column] - data[column].mean()) / data[column].std())
        #     outlier_count = (z_scores > 30).sum()  # You can adjust the threshold (e.g., 3) for outliers
        # else:
        #     outlier_count = -1

        # Append the results to the report DataFrame
        report_df = pd.concat([report_df, pd.DataFrame({
            'File Name': file_name,
            'Column Name': [column],
            'NaN Count': [nan_count],
            'NaN Percentage': [nan_percentage],
            'Data Types': [data_type],
            'Duplicate Rows': [duplicate_rows],
            # 'Outliers': [outlier_count]
        })], ignore_index=True)

        # Create a box plot for outliers
        # if outlier_count > 0:
        #     plt.figure(figsize=(10, 6))
        #     sns.boxplot(data=data, x=column)
        #     plt.title(f'Box Plot for {file_name}_{column}')
        #     plt.xlabel(column)
        #     plt.ylabel('Values')
        #     plot_filename = f"{file_name}_{column}_boxplot.png"
        #     plt.savefig(plot_filename)
        #     plt.show()

    return report_df


# Function to create descriptive statistics and data visualizations
def create_statistics_and_visualization(file_name, data):
    # Filter the DataFrame to include only numeric columns
    numeric_columns = data.select_dtypes(include='number')

    # Get descriptive statistics
    stats_df = numeric_columns.describe()
    stats_df['File'] = [file_name for _ in range(len(stats_df))]

    file_name = file_name[:-4]

    # Plot numeric variables against time
    if 'time' in data.columns:
        for column in numeric_columns.columns:
            if column == 'timestamp':
                continue
            plt.figure(figsize=(10, 6))
            plt.plot(data['time'], numeric_columns[column])
            plt.title(f'{column} Over Time')
            plt.xlabel('Time')
            plt.ylabel(column)

            # Save the plot
            plot_filename = f"{file_name}_{column}_timeline.png"
            if 'trades' in file_name:
                plt.savefig(f'{event}_trades_visualizations/' + plot_filename)
            elif 'orderbooks' in file_name:
                plt.savefig(f'{event}_orderbooks_visualizations/' + plot_filename)
            else:
                plt.savefig(f'{event}_quotes_visualizations/' + plot_filename)
            plt.show()

            sns.histplot(data[column], bins=20)
            plt.title(f'{column} Distribution')
            plt.xlabel(column)
            plt.ylabel('Frequency')

            plot_filename = f"{file_name}_{column}_distribution.png"
            if 'trades' in file_name:
                plt.savefig(f'{event}_trades_visualizations/' + plot_filename)
            elif 'orderbooks' in file_name:
                plt.savefig(f'{event}_orderbooks_visualizations/' + plot_filename)
            else:
                plt.savefig(f'{event}_quotes_visualizations/' + plot_filename)
            plt.show()

            # Create a sheet for data visualization and insert the plot
            #worksheet = writer.book.add_worksheet(f'{sheet_name} Data Visualization - {column}')
            #worksheet.insert_image('A2', plot_filename)

            # Close the plot
            plt.close()

    return stats_df

    # Save the statistics to the Excel file
    # stats_df.to_excel(writer, sheet_name=f'Descriptive Statistics', index=True)

# Specify the folder containing the data files
data_folder = f'{event}_data'

# Get a list of CSV files in the folder
data_files = [os.path.join(data_folder, file) for file in os.listdir(data_folder) if file.endswith('.csv')]

# Create an empty Pandas DataFrame to store the reports
all_data = {}

# Create separate lists for trade and order book data
trade_data = []
orderbook_data = []
quotes_data = []

# Iterate through each data file and create a report
for data_file in data_files:
    data = pd.read_csv(data_file)
    file_name = os.path.basename(data_file)
    all_data[file_name] = data

    if 'trades' in file_name:
        trade_data.append(data)
    elif 'orderbooks' in file_name:
        orderbook_data.append(data)
    else:
        quotes_data.append(data)

# Specify the name of the Excel file for the combined report
excel_filename = f'{event}_combined_data_preparation_report.xlsx'

all_statistics = []

# Create a Pandas ExcelWriter to save all reports into one Excel file
with pd.ExcelWriter(excel_filename, engine='xlsxwriter') as writer:
    for data, sheet_name in zip([trade_data, orderbook_data, quotes_data], ['Trade Data Reports', 'Order Book Data Reports', 'Quote Data Reports']):
        all_reports_df = pd.concat(
            [create_data_report(data, file_name) for data, file_name in zip(data, all_data.keys())])
        all_reports_df.to_excel(writer, sheet_name=sheet_name, index=False)
    for file_name, data in all_data.items():
        # Create a separate sheet for statistics
        all_statistics.append(
            create_statistics_and_visualization(file_name, data)
        )
    all_statistics_df = pd.concat(all_statistics)
    all_statistics_df.to_excel(writer, sheet_name='Descriptive Statistics', index=False)


print(f'Combined report generated and saved as {excel_filename}')











