########################################################################################################################################################################################################
#Import necessary libraries
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging, time
import json
import pandas as pd
from sqlalchemy import text
import eventlet
from eventlet.green.urllib.request import urlopen
from eventlet.green.urllib.error import HTTPError
########################################################################################################################################################################################################
#Configuration
csv_path = '/home/octo_26/.local/lib/python3.12/site-packages/airflow/dags/data'

table_name = 'unique_historical_stock'
company = pd.read_csv(f'{csv_path}/Urls_daily.csv')
urls = company['url']

def get_pg_conn():
    con = PostgresHook(postgres_conn_id='postgres_conn')
    return con

conn = get_pg_conn().get_sqlalchemy_engine()
########################################################################################################################################################################################################
#DAG Definition
default_args = {
    'owner': 'admin',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=0.2)
}

dag = DAG(
    'ETL_Daily_Data',
    default_args = default_args,
    description = 'Extract daily data',
    catchup=True,
    schedule_interval = "0 16 * * Mon-Fri",
)
########################################################################################################################################################################################################
#Function Definition

def fetch(url):
    try:
        with urlopen(url) as response:
            logging.info(f"Successfully read {url}")
            return response.read().decode('utf-8')
    except HTTPError as e:
        if e.code == 429:  
            sleep_time = 0.1
            logging.error(f"Rate limit hit for {url}, retrying after {sleep_time} sec...")
            print(f"Rate limit hit for {url}, retrying after {sleep_time} sec...")
            time.sleep(sleep_time)  
            return fetch(url) 
        else:
            logging.error(f"HTTPError occurred: {e}")
            print(f"HTTPError occurred: {e}")
            return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
        return None
    
def fetch_urls(urls):
    pool = eventlet.GreenPool()
    all_responses = []
    multiple_responses = [pool.spawn(fetch, url) for url in urls]

    multiple_responses = [response.wait() for response in multiple_responses]
    all_responses.extend(multiple_responses)
    return all_responses

def read_urls():
    responses = fetch_urls(urls)
    return responses
 
def extract(l_dict):
    df = pd.DataFrame(columns=['ticker', 'open', 'low', 'high', 'close', 'volume', 'date'])
    for dict_string in l_dict:
        dict = json.loads(dict_string)
        try:
            data_dict = {
                'ticker': dict['ticker'],
                'open': dict["data"][0]["open"],
                'low': dict["data"][0]["low"],
                'high': dict["data"][0]["high"],
                'close': dict["data"][0]["close"],
                'volume': dict["data"][0]["volume"], 
                'date': dict["data"][0]["tradingDate"]
            }
        except IndexError as i:
            data_dict = {
                'ticker': dict['ticker'],
                'open': None,
                'low': None,
                'high': None,
                'close': None,
                'volume': None, 
                'date': None
            }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    df['date'] = df['date'].apply(lambda x: 
                                  datetime.strptime(x.replace("T", " ").replace(".000Z", ""), "%Y-%m-%d %H:%M:%S")
                                  + timedelta(hours=7) if x != None else x)
    df['transaction_id'] = df['ticker'] + df['date'].dt.year.astype(str) + df['date'].dt.month.astype(str) + df['date'].dt.day.astype(str) 
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def load(df):
    global conn
    with conn.connect() as connection:
        trans = connection.begin()  
        for value in df.values.tolist():
            query = f"INSERT INTO {table_name} (ticker, open, high, low, close, volume, date, transaction_id) VALUES {tuple(value)}"
            try:
                connection.execute(text(query))
                trans.commit()
                print(f"Ticker {value[0]} was appended into database")
            except Exception as e:
                print(f"Error: {e}")
                trans.rollback()

def etl():
    responses = read_urls()
    load(transform(extract(responses)))
    
########################################################################################################################################################################################################
#Main


etl_daily = PythonOperator(
    task_id = 'ETL_daily',
    dag = dag,
    python_callable=etl,
)

etl_daily
