from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

import pandas as pd
import urllib3, json, time

http = urllib3.PoolManager()
df = pd.DataFrame(columns=['ticker', 'url'])
now = datetime.now()
unix_now = int(datetime.timestamp(now))
today_21h = now.replace(hour=21, minute=0, second=0, microsecond=0)
if now > today_21h:
    today_21h += timedelta(days=1)
unix_21h = int(today_21h.timestamp())
csv_path = '/home/octo_26/.local/lib/python3.12/site-packages/airflow/dags/data'
company = pd.read_csv(f'{csv_path}/Company.csv')
########################################################################################################################################################################################################
#DAG Definition
default_args = {
    'owner': 'admin',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=0.2)
}

dag = DAG(
    'Get_daily_all_urls',
    default_args = default_args,
    description = 'Get_daily_all_urls',
    schedule_interval = "0 15 * * Mon-Fri",
)

########################################################################################################################################################################################################
def get_urls_daily(): 
    global df
    for ticker in company['ticker']:
        data_dict = {
            'ticker': ticker,
            'url': f'https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/bars-long-term?resolution=D&ticker={ticker}&type=stock&to={unix_21h}&countBack=1'
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    df.to_csv(f'{csv_path}/Urls_daily.csv', index=False)

make_url = PythonOperator(
    task_id = 'Get_urls_daily',
    dag = dag,
    python_callable=get_urls_daily
)

make_url
