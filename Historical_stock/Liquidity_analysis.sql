--Find top 30 stocks by volume traded for each year after 2019.
WITH cte as (
	SELECT ticker,
		date_part('year', date) as year, 
		SUM(volume) as total_volume,
		ROW_NUMBER() OVER (PARTITION BY date_part('year', date) ORDER BY SUM(volume) DESC) as RowNum
	FROM unique_historical_stock
	WHERE date_part('year', date)>= 2016 
	GROUP BY ticker, year
	ORDER BY ticker ASC, year DESC, total_volume DESC
)
SELECT cte.year, cte.ticker FROM cte
WHERE cte.rownum <=30
ORDER BY year DESC, rownum ASC

--Create a view for the query above
CREATE VIEW public.Top_30_tickers_most_liquidity_from_2016
AS
	WITH cte as (
		SELECT ticker,
			date_part('year', date) as year, 
			SUM(volume) as total_volume,
			ROW_NUMBER() OVER (PARTITION BY date_part('year', date) ORDER BY SUM(volume) DESC) as RowNum
		FROM unique_historical_stock
		WHERE date_part('year', date)>= 2016 
		GROUP BY ticker, year
		ORDER BY ticker ASC, year DESC, total_volume DESC
	)
	SELECT cte.year, cte.ticker FROM cte
	WHERE cte.rownum <=30
	ORDER BY year DESC, rownum ASC

ALTER TABLE public.'Top_30_tickers_most_liquidity_from_2016'
    OWNER TO postgres;

--Find the percentage difference in volume traded for each stock from the previous month.
WITH cte as (
	SELECT ticker,
		date_part('month', date) as month,
		date_part('year', date) as year,
		SUM(volume) as volume,
		LAG(SUM(volume)) OVER (Partition by ticker ORDER BY date_part('year', date) ASC, date_part('month', date) ASC)
	FROM unique_historical_stock
	GROUP BY ticker, month, year
	ORDER BY ticker ASC, year ASC, month ASC
),
cte2 as (
	SELECT *, 
		CASE 
			WHEN lag !=0 
			THEN ROUND(CAST((volume-lag)/lag * 100.0 as numeric), 2) 
			ELSE 0 
		END as percentage_diff
	FROM cte
)
SELECT * FROM cte2



SELECT 
    ticker,
    date,
    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30
	
FROM unique_historical_stock;

--Find invalid tickers that don't actively trade anymore.
SELECT ticker, MAX(date) as date
FROM unique_historical_stock
GROUP BY ticker
HAVING MAX(date) != (SELECT MAX(date) FROM unique_historical_stock)
ORDER BY MAX(date) ASC

--	Create a view for the query above
CREATE VIEW public."Invalid_tickers"
AS
	SELECT ticker, MAX(date) as date
	FROM unique_historical_stock
	GROUP BY ticker
	HAVING MAX(date) != (SELECT MAX(date) FROM unique_historical_stock)
	ORDER BY MAX(date) ASC;

ALTER TABLE public."Invalid_tickers"
    OWNER TO postgres;

--Find bad liquidity tickers that have many consecutive days without transactions.
SELECT ticker, COUNT(*) as zero_volume_days
FROM unique_historical_stock
WHERE volume = 0 AND date >= (SELECT MAX(date) FROM unique_historical_stock) - INTERVAL '30 days'
GROUP BY ticker
HAVING COUNT(*) >= 20
ORDER BY zero_volume_days DESC

--	Create a view for the query above
CREATE VIEW public."Bad_liquidity_tickers"
AS
	SELECT ticker, COUNT(*) as zero_volume_days
	FROM unique_historical_stock
	WHERE volume = 0 AND date >= (SELECT MAX(date) FROM unique_historical_stock) - INTERVAL '30 days'
	GROUP BY ticker
	HAVING COUNT(*) >= 20
	ORDER BY zero_volume_days DESC

ALTER TABLE public."Bad_liquidity_tickers"
    OWNER TO postgres;