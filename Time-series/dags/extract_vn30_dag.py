from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

import pandas as pd
import time
import urllib3, json

http = urllib3.PoolManager()
BASE_URL = "https://trading.vietcap.com.vn/api/price/symbols/getByGroup?group=VN30"
df = pd.DataFrame(columns=['ticker', 'url'])
now = datetime.now().replace(second=49)
unix_now = int(datetime.timestamp(now))
csv_path = '/home/octo_26/.local/lib/python3.12/site-packages/airflow/dags/data'
########################################################################################################################################################################################################
#DAG Definition
default_args = {
    'owner': 'admin',
    'start_date':days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=0.2)
}

dag = DAG(
    'Get_urls_vn30',
    default_args = default_args,
    description = 'Get_urls_of_30_biggest_tickers',
    schedule_interval = "*/1 2-4,6-7 * * 1-5",
    catchup=False
)
########################################################################################################################################################################################################
def get_vn30(): 
    global df
    responses = http.request('GET', BASE_URL).data.decode('utf-8')
    json_text = json.loads(responses)
    for ticker in json_text:
        data_dict = {
            'ticker': ticker['symbol'],
            'url': f'https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/bars?resolution=1&ticker={ticker['symbol']}&type=stock&to={unix_now}&countBack=1'
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    df.to_csv(f'{csv_path}/VN30.csv', index=False)

def wait_until_sec_59():
    time.sleep(59 - 1 - 3 - datetime.now().second)

wait = PythonOperator(
    task_id = 'wait_until_sec_59',
    dag = dag,
    python_callable=wait_until_sec_59
)

get_url_vn30 = PythonOperator(
    task_id = 'Get_urls_vn30',
    dag = dag,
    python_callable=get_vn30
)

etl_vn30 = TriggerDagRunOperator(
    task_id = 'trigger_etl_stage',
    trigger_dag_id = "ETL_Stage",
)
get_url_vn30 >> wait >> etl_vn30
