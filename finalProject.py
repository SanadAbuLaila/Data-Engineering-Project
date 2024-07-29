# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
import sqlite3 as sql
from datetime import datetime

# init all known variables

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
final_table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path = './Largest_banks_data.csv'
database = 'Banks.db'
table = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%b-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, 'a') as f:  # open the log file in append mode
        f.write(f'{timestamp} : {message}\n')
    print(f'{timestamp} : {message}\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    # Initialize the DataFrame
    df = pd.DataFrame(columns=table_attribs)
    # Fetch the page content
    page = requests.get(url).text
    # Parse the HTML content
    content = bs(page, 'html.parser')
    # Find the table with class 'wikitable'
    table = content.find('table', {'class': 'wikitable'})
    if not table:
        print("Table not found.")
        return df
    # Extract rows from the table
    rows = table.find_all('tr')[1:]  # Skip header row
    # List to collect row data
    data = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            try:
                name = cols[1].get_text(strip=True)
                mc_usd_billion = cols[2].get_text(strip=True).replace(',', '').replace(' billion', '')
                mc_usd_billion = float(mc_usd_billion)
                data.append({'Name': name, 'MC_USD_Billion': mc_usd_billion})
            except Exception as e:
                print(f"Error: {e}")
    # Convert list to DataFrame
    df = pd.DataFrame(data, columns=table_attribs)
    print(df)
    return df


def transform(df, exchange_csv_path):
    ''' This function transforms the DataFrame by adding columns for different currencies 
    based on exchange rates read from the CSV file. '''

    # Read the exchange rate CSV file into a DataFrame
    exchange_df = pd.read_csv(exchange_csv_path)
    # Convert the exchange rate DataFrame into a dictionary
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    # Add MC_GBP_Billion column
    df['MC_GBP_Billion'] = df['MC_USD_Billion'].copy()  # Copy to avoid modifying the original
    for i in range(len(df)):
        df.at[i, 'MC_GBP_Billion'] = round(df.at[i, 'MC_USD_Billion'] * exchange_rate.get('GBP', 1), 2)
    # Add MC_EUR_Billion column
    df['MC_EUR_Billion'] = df['MC_USD_Billion'].copy()  # Copy to avoid modifying the original
    for i in range(len(df)):
        df.at[i, 'MC_EUR_Billion'] = round(df.at[i, 'MC_USD_Billion'] * exchange_rate.get('EUR', 1), 2)
    # Add MC_INR_Billion column
    df['MC_INR_Billion'] = df['MC_USD_Billion'].copy()  # Copy to avoid modifying the original
    for i in range(len(df)):
        df.at[i, 'MC_INR_Billion'] = round(df.at[i, 'MC_USD_Billion'] * exchange_rate.get('INR', 1), 2)
    print(df)
    print("Market capitalization of the 5th largest bank in billion EUR:", df['MC_EUR_Billion'].iloc[4])
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    # Cursor object
    cursor = sql_connection.cursor()
    # Execute statement
    cursor.execute(query_statement)
    # To save results
    results = cursor.fetchall()
    # Confirmation
    print(f"Executing -> {query_statement}")
    # To print results
    for row in results:
        print(row)
    # Close
    cursor.close()    

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process.')

transform(df, exchange_csv_path)
log_progress('Data transformation complete. Initiating Loading process.')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file.')

sql_connection = sql.connect(database)
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table)
log_progress('Data loaded to Database as a table, Executing queries.')

q1 = 'SELECT * FROM Largest_banks'
run_query(q1, sql_connection)
log_progress('Process Complete')

q2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(q2, sql_connection)
log_progress('Process Complete')

q3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(q3, sql_connection)
log_progress('Process Complete')