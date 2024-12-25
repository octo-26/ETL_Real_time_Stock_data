-- Description: This script cleans the historical stock data by removing duplicates, adding indexes, and checking for data errors. Although the data was ETL, it can't avoid duplication of the last transaction of bad tickers which stopped trading or got banned. Besides, few tickers were recorded with wrong values.
SELECT DISTINCT * 
INTO unique_historical_stock
FROM historical_stock

-- Add index to columns ticker and date to improve query performance
CREATE INDEX idx_ticker ON historical_stock(ticker);
CREATE INDEX idx_date ON historical_stock(date);

-- Check for duplicates
SELECT ticker, date FROM unique_historical_stock
GROUP BY ticker,date
HAVING COUNT(ticker) > 1

-- Query data errors. 
SELECT * FROM unique_historical_stock
WHERE ticker = 'HNG' AND date BETWEEN '2024-10-20' AND '2024-10-22'

-- Remove error
DELETE FROM unique_historical_stock
WHERE ticker ='HNG' AND date BETWEEN '2024-10-20' AND '2024-10-22' AND close = 4500

-- Update correct values 
UPDATE unique_historical_stock 
SET close = 4500
WHERE ticker = 'HNG' AND date BETWEEN '2024-10-20' AND '2024-10-22'

-- Query data errors. 
SELECT * FROM unique_historical_stock
WHERE ticker = 'HBC' AND date BETWEEN '2024-10-20' AND '2024-10-22'

-- Remove errors
DELETE FROM unique_historical_stock
WHERE ticker ='HBC' AND date BETWEEN '2024-10-20' AND '2024-10-22' AND close = 4900

-- Update correct values 
UPDATE unique_historical_stock 
SET close = 4900
WHERE ticker = 'HBC' AND date BETWEEN '2024-10-20' AND '2024-10-22'