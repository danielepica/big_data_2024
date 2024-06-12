SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET hive.auto.convert.join=false;


-- Creazione della tabella 'historical_stock_prices' con la definizione dei campi e delle proprietà della tabella
CREATE TABLE historical_stock_prices(
    ticker STRING,
    open FLOAT,
    close FLOAT,
    low FLOAT,
    high FLOAT,
    volume FLOAT,
    data DATE,
    name STRING,
    sector STRING,
    industry STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

-- Caricamento dei dati dal file locale nella tabella 'historical_stock_prices'
LOAD DATA LOCAL INPATH '100k_historical_stock_prices_merged.csv' OVERWRITE INTO TABLE historical_stock_prices;



CREATE TABLE historical_stock_open_close AS
    SELECT
        hsp.sector as sector,
        hsp.industry as industry,
           year(hsp.data) as year,
           sum(hsp.open) as sum_open,
           sum(hsp.close) as sum_close
    FROM historical_stock_prices hsp
    where hsp.industry != ''
    GROUP BY hsp.sector,hsp.industry, year(hsp.data);

-- Per trovare l'azione con la più alta variazione percentuale nell'anno.
CREATE TABLE percentage_change_industry AS
    SELECT sector as sector,
           industry as industry,
           year as year,
           ROUND(((sum_close - sum_open)/ sum_open) * 100,1) as percentage_change_max
    FROM historical_stock_open_close
    WHERE industry != ''
    ORDER BY percentage_change_max DESC;
-- Quindi qui ho industry, year e variazione dell'industry per quel year

--- Per trovare l'azione con la più alta variazione percentuale nell'anno faccio prima somma close e open
CREATE TABLE ticker_open_close AS
    SELECT hsp.industry,
           hsp.ticker as ticker,
           year(hsp.data) as year,
           sum(hsp.open) as open_sum,
           sum(hsp.close) as close_sum
    FROM historical_stock_prices hsp
    WHERE hsp.industry != ''
    GROUP BY hsp.industry, hsp.ticker, year(hsp.data);

--- Per trovare l'azione con la più alta variazione percentuale nell'anno.
CREATE TABLE percentage_change_ticker AS
    SELECT industry,
           ticker as ticker,
           year as year,
           ROUND(((close_sum - open_sum)/ open_sum) * 100,1) as percentage_change_max
    FROM ticker_open_close hsp
    WHERE industry != ''
    ORDER BY industry, percentage_change_max DESC;
-- Quindi qui ho industry, year, ticker e variazione del ticker per quel year, ed è ordinato per variazione decrescente quindi il primo ticker è quello con la variazione maggiore in quell'anno


--Tabella per avere il max volume e il ticker associato
 CREATE TABLE volume_ticker AS
    SELECT hsp.industry,
           hsp.ticker as ticker,
           year(hsp.data) as year,
           avg(hsp.volume) as total_volume
    FROM historical_stock_prices hsp
    WHERE hsp.industry != ''
    GROUP BY hsp.industry, hsp.ticker, year(hsp.data)
    ORDER BY total_volume DESC;

-- Quindi qui ho per ogni ticker e anno il volume, ed è ordinato per volume decrescente quindi il primo ticker è quello con il volume maggiore in quell'anno

--Metto tutto insieme

CREATE TABLE industry_statistics AS
SELECT
    hsp.sector as sector,
    hsp.industry AS industry,
    hsp.year as year,
    hsp.percentage_change_max as percentage_change_industry,
    max(pct.percentage_change_max) as percentage_change_ticker,
    max(vt.total_volume)  AS total_volume
FROM
    percentage_change_industry hsp
JOIN
    percentage_change_ticker pct ON pct.industry = hsp.industry AND pct.year = hsp.year
JOIN
    volume_ticker vt ON vt.industry = hsp.industry AND vt.year = hsp.year
WHERE
    hsp.industry != ''
GROUP BY hsp.sector, hsp.industry, hsp.year, hsp.percentage_change_max
;

CREATE TABLE final_statistics AS
SELECT
    hsp.sector as sector,
    hsp.industry AS industry,
    hsp.year as year,
    hsp.percentage_change_industry as percentage_change_industry,
    pct.ticker as ticker_with_max_percentage_change,
    hsp.percentage_change_ticker as percentage_change_ticker,
    vt.ticker as ticker_with_max_volume,
    hsp.total_volume  AS total_volume
FROM
    industry_statistics hsp
JOIN
    (SELECT pct1.industry, pct1.year, pct1.percentage_change_max, pct1.ticker
     FROM percentage_change_ticker pct1
     JOIN (SELECT industry, year, percentage_change_max, MIN(ticker) AS ticker
           FROM percentage_change_ticker
           GROUP BY industry, year, percentage_change_max) pct2
     ON pct1.industry = pct2.industry
     AND pct1.year = pct2.year
     AND pct1.percentage_change_max = pct2.percentage_change_max
     AND pct1.ticker = pct2.ticker) pct
ON pct.industry = hsp.industry AND pct.year = hsp.year AND pct.percentage_change_max = hsp.percentage_change_industry
JOIN
    (SELECT vt1.industry, vt1.year, vt1.total_volume, vt1.ticker
     FROM volume_ticker vt1
     JOIN (SELECT industry, year, total_volume, MIN(ticker) AS ticker
           FROM volume_ticker
           GROUP BY industry, year, total_volume) vt2
     ON vt1.industry = vt2.industry
     AND vt1.year = vt2.year
     AND vt1.total_volume = vt2.total_volume
     AND vt1.ticker = vt2.ticker) vt
ON vt.industry = hsp.industry AND vt.year = hsp.year AND vt.total_volume = hsp.total_volume
WHERE
    hsp.industry != '';

drop table historical_stock_prices;
drop table historical_stock_open_close;
drop table percentage_change_industry;
drop table ticker_open_close;
drop table percentage_change_ticker;
drop table volume_ticker;
drop table industry_statistics;
drop table final_statistics;