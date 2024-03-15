import requests
import pandas as pd
import numpy as np
import os
import sqlite3
import urllib3
from tqdm.auto import tqdm

# **GitHub repository and path information**
GITHUB_OWNER = 'CSSEGISandData'
GITHUB_REPOSITORY = 'COVID-19'
DATA_PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
API_URL = f'https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPOSITORY}/contents/{DATA_PATH}'

print(f'Downloading paths from {API_URL}')

# **Collect download URLs for CSV files**
download_urls = []
response = requests.get(API_URL)
for file_data in tqdm(response.json()):
    if file_data['name'].endswith('.csv'):
        download_urls.append(file_data['download_url'])

# **Column relabeling dictionary for standardization**
column_renaming = {
    'Country/Region': 'Country_Region',
    'Lat': 'Latitude',
    'Long_': 'Longitude',
    'Province/State': 'Province_State',
}


def prepare_covid_data_for_sql(data_frame, filename):
    """Restructures a DataFrame to be uploaded to a SQL database.

    Args:
        data_frame (pandas.DataFrame): The DataFrame containing the COVID data.
        filename (str): The filename of the data source, used for date extraction.

    Returns:
        pandas.DataFrame: The DataFrame with standardized columns and prepared for SQL upload.
    """

    # Rename columns according to the relabeling dictionary
    data_frame.rename(columns=column_renaming, inplace=True)

    # Ensure critical columns are present
    essential_columns = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered']
    for column in essential_columns:
        if column not in data_frame:
            data_frame[column] = np.nan

    # Add 'Last_Update' column from filename if missing
    if 'Last_Update' not in data_frame:
        data_frame['Last_Update'] = pd.to_datetime(filename)

    return data_frame[essential_columns]


def upload_data_to_sql(file_paths, database_name, debug=False):
    """Uploads data from a list of file paths to a SQLite database.

    Args:
        file_paths (list): A list of file paths to be uploaded.
        database_name (str): The name of the SQLite database.
        debug (bool, optional): True to enable debug messages. Defaults to False.
    """

    database_connection = sqlite3.connect(f"{database_name}.db")

    if debug:
        print("Uploading into database")

    for i, file_path in tqdm(enumerate(file_paths)):

        data = pd.read_csv(file_path)
        filename = os.path.basename(file_path).split('.')[0]
        data = prepare_covid_data_for_sql(data, filename)

        if i == 0:  # For the first file, create a new table
            data.to_sql(database_name, con=database_connection, index=False, if_exists='replace')
        else:  # For subsequent files, append to the existing table
            data.to_sql(database_name, con=database_connection, index=False, if_exists='append')


# Upload the downloaded data to the SQLite database
upload_data_to_sql(download_urls, 'covid_data', debug=True)