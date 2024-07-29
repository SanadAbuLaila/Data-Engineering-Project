# import the required libraries

from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np
import sqlite3 as sql
from datetime import datetime 

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text # get the webpage
    data = bs(page,'html.parser')  # parse using BeautifulSoup
    df = pd.DataFrame(columns=table_attribs) #init the dataframe
    tables = data.find_all('tbody') # our target is a table from the tables
    rows = tables[2].find_all('tr') # extracting rows from target table
    for row in rows: # verification loop
        col = row.find_all('td') # save all data per row as list in col
        if len(col)!=0: # make sure col (row contents) is not null
            """ make sure the row has working link in [0] and gdp in [2] is not null indicated by '—' """
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # extract the data into list format
    GDP_list = df["GDP_USD_millions"].tolist()
    
    # clean each element in the list (1,234 <- currency format into -> 1234.0 float format)
    cleaned_list = []  # define cleaned list
    for element in GDP_list:
        cleaned_element = "".join(element.split(','))  # split number using  ',' separator then join using ""
        cleaned_list.append(float(cleaned_element))

    # convert from million to billion and round to 2 decimal places
    GDP_USD_billions = []  # to append the processed data
    for element in cleaned_list:
        processed_element = element / 1000  # convert from millions to billions
        rounded_element = round(processed_element, 2)  # round to 2 decimal places
        GDP_USD_billions.append(rounded_element)
    
    # update the given dataframe in function's argument
    df['GDP_USD_millions'] = GDP_USD_billions

    # Rename the column to reflect the new unit (billions)
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sql.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()