import urllib3
import json
import pandas as pd

http = urllib3.PoolManager()
url = 'https://ai.vietcap.com.vn/api/get_all_tickers'
df = pd.DataFrame(columns=['ticker','company'])
response = http.request('GET', url).data.decode('utf-8')
ticker = json.loads(response)
length = int(ticker['record_count'])
for i in range(1,length):
    data_dict = {
        'ticker': ticker['ticker_info'][i]['ticker'],
        'company': ticker['ticker_info'][i]['organ_name'],
    }
    df1 = pd.DataFrame(data_dict, index=[0])
    df = pd.concat([df,df1], ignore_index=True)

df.to_csv('Company.csv', index=False)
