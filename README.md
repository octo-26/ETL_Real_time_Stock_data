# ETL_Real_time_Stock_data

<!-- OVERVIEW -->
## Project Overview

In this project, I implemented an ETL pipeline using Apache Airflow to extract real-time stock data from the public API provided by TCBS, transform it into a suitable format, and load it into a local PostgreSQL database. The project was initialized using Airflow to schedule extracting data of VN30 every 1 minutes when the market is available and all components using in the project was installed on my local machine. Besides, Airflow is also applied to ETL daily data and load into a historical database for further analysis.

VN30 - Index of the 30 largest companies by market capitalization and liquidity listed on the Ho Chi Minh City Stock Exchange (HOSE) in Vietnam

<!-- ARCHITECTURE DIAGRAM -->
## Architecture diagram

![Architecture](./Architecture.png)

<!-- OBJECTIVE-->
## Objective

- Built an ETL pipeline using Apache Airflow to automate the process of extracting real-time stock data every 1 minute and current stock data daily. 
- Store the processed data in a PostgreSQL database.
- Analysis based on historical data.

<!-- LIMITATION -->
## Limitation

- The project has not dealt with API Rate Limit yet, which leads to issues in extract real-time stock data of the entire tickers.
- The data is not entirely accurate, specifically the volume, as it is not possible to crawl the exact figures due to the nature of the API.
