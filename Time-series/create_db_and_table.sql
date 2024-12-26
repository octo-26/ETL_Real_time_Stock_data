CREATE DATABASE stock_1m;

-- Create table for 15 min stock data
CREATE TABLE public."stock"
(
    ticker text,
    open double precision,
    low double precision,
    high double precision,
    close double precision,
    volume bigint,
    date timestamp without time zone
);

ALTER TABLE IF EXISTS public."15_min_stock_table"
    OWNER to postgres;

-- Create table for historical stock data
CREATE TABLE public."historical_stock"
(
    ticker text,
    open double precision,
    low double precision,
    high double precision,
    close double precision,
    volume bigint,
    date timestamp without time zone
);

ALTER TABLE IF EXISTS public."historical_stock"
    OWNER to postgres;
