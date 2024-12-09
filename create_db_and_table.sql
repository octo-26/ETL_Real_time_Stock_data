CREATE DATABASE stock_1m;

CREATE TABLE public."stockseries"
(
    ticker text,
    open double precision,
    low double precision,
    high double precision,
    close double precision,
    volume bigint,
    date timestamp without time zone
);

ALTER TABLE IF EXISTS public."stockseries"
    OWNER to postgres;
