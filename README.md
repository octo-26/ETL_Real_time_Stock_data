# ETL_Real_time_Stock_data
## Project Overview

In this project, I implemented an ETL pipeline using Apache Airflow to extract real-time stock data from the public API provided by TCBS, transform it into a suitable format, and load it into a local PostgreSQL database. The project was initialized using Airflow to schedule extracting data of VN30 every 1 minutes when the market is available and all components using in the project was installed on my local machine. 

VN30 - Index of the 30 largest companies by market capitalization and liquidity listed on the Ho Chi Minh City Stock Exchange (HOSE) in Vietnam
## Objective
### The main objective of this project was to:

- Built an ETL pipeline using Apache Airflow to automate the process of extracting real-time stock data every 1 minute.
- Store the processed data in a PostgreSQL database.
