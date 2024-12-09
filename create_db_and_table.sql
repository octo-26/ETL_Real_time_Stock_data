CREATE DATABASE table;

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