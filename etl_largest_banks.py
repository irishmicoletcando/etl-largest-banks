import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required information from the website and save it to a data frame. The function returns the data frame for further processing. '''
    
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table')
    print(f"Total tables found on the webpage: {len(tables)}")

    # Find the specific table by matching the class attributes
    market_capitalization_table = None
    for table in tables:
        if 'wikitable' in table.get('class', []) and 'sortable' in table.get('class', []):
            market_capitalization_table = table
            break

    if market_capitalization_table:
        print("Target table found on the webpage.")
        table_body = market_capitalization_table.find('tbody')
        rows = table_body.find_all('tr')
        
        for row in rows:
            col = row.find_all('td')
            if len(col) >= 3:
                try:
                    bank_name = col[1].get_text(strip=True)
                    mc_usd_billion = col[2].text.strip().replace(',', '')
                    mc_usd_billion = float(mc_usd_billion)
                    data_dict = {"Name": bank_name,
                                 "MC_USD_Billion": mc_usd_billion}
                    print(f"Extracted row: {data_dict}")
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
                except Exception as e:
                    log_progress(f"Error extracting data: {str(e)}")
                    print(f"Error extracting data: {str(e)}")
    else:
        log_progress("No table found on the webpage.")
        print("No table found on the webpage.")
    
    log_progress("Data extraction complete. Initiating Transformation process")
    print("Extracted DataFrame:\n", df)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each containing the transformed version of Market Cap column to respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    print("Exchange Rate DataFrame:\n", exchange_df)
    
    exchange_rate = exchange_df.set_index('Currency')['Rate'].to_dict()
    print("Exchange Rate Dictionary:\n", exchange_rate)
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    log_progress("Data transformation complete. Initiating Loading process")
    print("Transformed DataFrame:\n", df)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("SQL Connection initiated")
    
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and prints the output on the terminal. Function returns nothing. '''
    log_progress("Data loaded to Database as a table, Executing queries")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

''' Here, you define the required entities and call the relevant functions in the correct order to complete the project. Note that this portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
df = transform(df, csv_path)
load_to_csv(df, output_path)
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)

query_1 = "SELECT * FROM Largest_banks"
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_3 = "SELECT Name FROM Largest_banks LIMIT 5"

run_query(query_1, sql_connection)
run_query(query_2, sql_connection)
run_query(query_3, sql_connection)
log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")
log_progress("ETL Job Ended")