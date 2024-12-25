from datetime import datetime, timedelta
import pandas as pd
import json
import warnings
import urllib3
from init import BASE_URL, conn
from sqlalchemy import text
http = urllib3.PoolManager()
# Tắt cảnh báo FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

#######################################################################################
def format_time(s):
    s = s.replace("T", " ").replace(".000Z", "")
    s = datetime.strptime(s, "%Y-%m-%d %H:%M:%S") + timedelta(hours=7)
    return s

def call_data(symbol: str, interval: str, end=None, count_back:int=None):
    """
        Trả về DataFrame chứa các mức giá của mã chứng khoán đó và thời gian
        Input:  
            Interval: Khung thời gian trích xuất dữ liệu giá lịch sử. Giá trị nhận: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M. Mặc định là "1D".

    """
    if count_back is None:
        count_back = 365
    else:
        count_back = count_back

    if interval in ["1", "5", "10", "30", "60"]:
        end_point = "bars"
    else:
        end_point = "bars-long-term"

    if end is None:
        end = str(datetime.now().strftime("%Y-%m-%d"))
        date_end = datetime.strptime(end, "%Y-%m-%d")
        end_stamp = int(date_end.timestamp())
    else:
        if str(end).startswith("-"):
            end = str((datetime.now()-timedelta(days=abs(int(end)))).strftime("%Y-%m-%d"))
            date_end = datetime.strptime(end, "%Y-%m-%d")
            end_stamp = int(date_end.timestamp())
        else:
            date_end = datetime.strptime(end, "%Y-%m-%d")
            end_stamp = int(date_end.timestamp())

    url = f"{BASE_URL}/{end_point}?resolution={interval}&ticker={symbol}&type=stock&to={end_stamp}&countBack={count_back}"
    return url

def read_text_to_df(url):
    response = http.request('GET', url).data.decode('utf-8')
    json_text = json.loads(response)
    length1 = len(json_text['data'])
    ticker = json_text['ticker']
    df = pd.DataFrame(columns=["ticker", "open", "high", "low", "close", "volume", "date"])   
    for i in range(length1):
        data_dict = {
            'ticker': ticker,
            'open': json_text['data'][i]['open'],
            'high': json_text['data'][i]['high'],
            'low': json_text['data'][i]['low'],
            'close': json_text['data'][i]['close'],
            'volume': json_text['data'][i]['volume'],
            'date': format_time(json_text['data'][i]['tradingDate']),
        }
        df = pd.concat([df,pd.DataFrame(data_dict, index=[0])], ignore_index=True)
    return df

def to_csv(ticker: str, interval, end = None, count_back: int = 365):
    url = call_data(ticker, interval, end, count_back)
    df = read_text_to_df(url)
    df.to_csv(f"stock_{end}.csv", index=False, mode='a', header=False)

def to_db(name: str, ticker: str, interval, end = None, count_back: int = 365, conn = conn):
    url = call_data(ticker, interval, end, count_back)
    df = read_text_to_df(url)
    df.to_sql(name=name, con=conn, if_exists="append", index=False)


#######################################################################################
"""
    Get the daily data of the stock
"""
def insert_query_daily(ticker: str,name: str, conn=conn):
    url = call_data(ticker, "D","2024-12-24" ,count_back=1)
    df = read_text_to_df(url)
    df['transaction_id'] = df['ticker'] + df['date'].dt.year.astype(str) + df['date'].dt.month.astype(str) + df['date'].dt.day.astype(str) 
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    query = f"INSERT INTO {name} (ticker, open, high, low, close, volume, date, transaction_id) VALUES {tuple(df.values.tolist()[0])}"
    try:
        conn.execute(text(query))
        conn.commit()
        print(f"Ticker {ticker} was appended into database")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

