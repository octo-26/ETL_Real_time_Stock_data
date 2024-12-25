from functions import to_csv, to_db
import pandas as pd
import time 
import logging 
from init import table_name, conn

company = pd.read_csv("Company.csv")
log_path = 'logs/'
logging.basicConfig(
    filename='historical_data.log',  
    filemode='a',               
    level=logging.DEBUG,        
    format='%(asctime)s - %(levelname)s - %(message)s',  
)

#start_time = time.time()
if 'ticker' in company.columns:
    for index, ticker in enumerate(company['ticker']):
        check=True
        sleep_time = 0.5
        while (check == True):
            try:
                # Append data into database
                to_db(ticker, "D","2010-12-04", count_back=365, conn=conn)
                info_string = f"Ticker {ticker} with {index} was appended into database"
                logging.info(info_string)
                print(f"{ticker} at index {index} was added into the database")
                check = False
            except KeyError as e:
                # Waiting for seconds if errors (mostly API Limit occur)
                error_string = f"KeyError: {e} - The column {ticker} might be missing or incorrectly named"
                logging.error(error_string)
                logging.info(f"Wait for {sleep_time} sec")
                print(f"Wait for {sleep_time} sec")
                time.sleep(sleep_time)
            except Exception as ex:
                # Waiting for seconds if other errors occur
                exception_string = f"KeyError: {ex} - The column {ticker} catch unknown error"
                logging.error(exception_string)
                logging.info(f"Wait for {sleep_time} sec")
                print(f"Wait for {sleep_time} sec")
                time.sleep(sleep_time)
                     
else:
    print("The 'ticker' column does not exist in the CSV file.")

#elapsed_time = time.time() - start_time
#print(elapsed_time)
