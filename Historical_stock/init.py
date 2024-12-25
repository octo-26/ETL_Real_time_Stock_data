from sqlalchemy import create_engine

BASE_URL = "https://apipubaws.tcbs.com.vn/stock-insight/v2/stock"
conn_string = 'postgresql+psycopg2://postgres:Octopus26abc@localhost:5432/stock_1m'
db = create_engine(conn_string)
conn = db.connect()

table_name = "historical_stock"