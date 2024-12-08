########################################################################################################################################################################################################
#Import necessary libraries
from datetime import datetime, timedelta
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging, time
import json
import pandas as pd
import requests
import eventlet
from eventlet.green.urllib.request import urlopen
from eventlet.green.urllib.error import HTTPError
########################################################################################################################################################################################################
#Configuration
csv_path = '/home/octo_26/.local/lib/python3.12/site-packages/airflow/dags/data'

logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
)

table_name = 'stockseries'
company = pd.read_csv(f'{csv_path}/Company2.csv')
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
    'email':['octo05025@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(seconds=0.2)
}

dag = DAG(
    'Extract_Stage',
    default_args = default_args,
    description = 'Extract_data',
    schedule_interval = "*/15 2-4,6-7 * * Mon-Fri",
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
    
def fetch_urls_in_batches(urls, batch_size=100):
    pool = eventlet.GreenPool()
    all_responses = []

    for i in range(0, len(urls), batch_size):
        batch = urls[i:i + batch_size]
        logging.info(f"Processing batch {i // batch_size + 1} with {len(batch)} URLs...")
        
        batch_responses = [pool.spawn(fetch, url) for url in batch]

        batch_responses = [response.wait() for response in batch_responses]
        all_responses.extend(batch_responses)

        if i < (len(urls))/batch_size -1:
            sleep_time = 5
            logging.info(f'Waiting between processing batches, about {sleep_time} sec')
            time.sleep(sleep_time)
    return all_responses

def read_urls():
    responses = fetch_urls_in_batches(urls[1:10])
    return responses
 
def extract(l_dict):
    df = pd.DataFrame(columns=['ticker', 'open', 'low', 'high', 'close', 'volume', 'date'])
    for dict_string in l_dict:
        dict = json.loads(dict_string)
        data_dict = {
            'ticker': dict['ticker'],
            'open': dict["data"][0]["open"],
            'low': dict["data"][0]["low"],
            'high': dict["data"][0]["high"],
            'close': dict["data"][0]["close"],
            'volume': dict["data"][0]["volume"], 
            'date': dict["data"][0]["tradingDate"]
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    df['date'] = df['date'].apply(lambda x:  
                                  datetime.strptime(
                                      x.replace("T", " ").replace(".000Z", ""), "%Y-%m-%d %H:%M:%S")
                                      + timedelta(hours=7)
                                      )

    return df

def load(df):
    global conn
    df.to_sql(name=table_name, con=conn, if_exists="append", index=False)

def etl():
    responses = read_urls()

    load(transform(extract(responses)))
    
########################################################################################################################################################################################################
#Main
etl = PythonOperator(
    task_id = 'ETL',
    dag = dag,
    python_callable=etl,
)   

etl