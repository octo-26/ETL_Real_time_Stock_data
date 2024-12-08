import requests
import json
import pandas as pd

url = 'https://ai.vietcap.com.vn/api/get_all_tickers'
df = pd.DataFrame(columns=['ticker','company'])

response = requests.get(url).text
ticker = json.loads(response)

length = int(ticker['record_count'])
for i in range(1,length):
    data_dict = {
        'ticker': ticker['ticker_info'][i]['ticker'],
        'company': ticker['ticker_info'][i]['organ_name']
    }
    df1 = pd.DataFrame(data_dict, index=[0])
    df = pd.concat([df,df1], ignore_index=True)

df.to_csv('Company.csv', index=False)