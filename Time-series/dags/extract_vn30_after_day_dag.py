from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

import pandas as pd
import urllib3, json, time

http = urllib3.PoolManager()
BASE_URL = "https://trading.vietcap.com.vn/api/price/symbols/getByGroup?group=VN30"
df = pd.DataFrame(columns=['ticker', 'url'])
now = datetime.now()
unix_now = int(datetime.timestamp(now))
today_21h = now.replace(hour=21, minute=0, second=0, microsecond=0)
if now > today_21h:
    today_21h += timedelta(days=1)
unix_21h = int(today_21h.timestamp())
csv_path = '/home/octo_26/.local/lib/python3.12/site-packages/airflow/dags/data'
########################################################################################################################################################################################################
#DAG Definition
default_args = {
    'owner': 'admin',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=0.2)
}

dag = DAG(
    'Get_urls_vn30_after_day',
    default_args = default_args,
    description = 'Get_urls_of_30_biggest_tickers_after_day',
    schedule_interval = "0 13 * * Mon-Fri",
)
########################################################################################################################################################################################################
def get_vn30_after_day(): 
    global df
    responses = http.request('GET', BASE_URL).data.decode('utf-8')
    json_text = json.loads(responses)
    for ticker in json_text:
        data_dict = {
            'ticker': ticker['symbol'],
            'url': f'https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/bars-long-term?resolution=D&ticker={ticker['symbol']}&type=stock&to={unix_21h}&countBack=1'
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    df.to_csv(f'{csv_path}/VN30_after_day.csv', index=False)

get_url_vn30_after_day = PythonOperator(
    task_id = 'Get_urls_vn30_after_day',
    dag = dag,
    python_callable=get_vn30_after_day
)

etl_vn30_after_day = TriggerDagRunOperator(
    task_id = 'trigger_etl_stage_after_day',
    trigger_dag_id = "ETL_Stage_after_day",
)

get_url_vn30_after_day >> etl_vn30_after_day
