
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
LOAD DATA LOCAL INPATH '/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/historical_stock_prices_merged.csv' OVERWRITE INTO TABLE historical_stock_prices;


-- Creazione della tabella 'historical_stock_prices_with_year' con l'aggiunta di una colonna 'year' derivata dalla data
CREATE TABLE historical_stock_prices_with_year AS
SELECT
    hsp.ticker,
    hsp.name,
    hsp.open,
    hsp.close,
    hsp.low,
    hsp.high,
    hsp.volume,
    hsp.data,
    YEAR(hsp.data) AS year
FROM
    historical_stock_prices hsp
ORDER BY hsp.ticker, hsp.data;


-- Creazione della tabella 'temp_start_end_dates' con le date di inizio e fine per ogni ticker e anno
CREATE TABLE temp_start_end_dates AS
SELECT
    ticker,
    year,
    MIN(data) AS first_date,
    MAX(data) AS last_date
FROM historical_stock_prices_with_year
GROUP BY ticker, year;

-- Creazione della tabella 'temp_first_last_close' con il primo e l'ultimo prezzo di chiusura per ogni ticker e anno
CREATE TABLE temp_first_last_close AS
SELECT
    a.ticker,
    a.year,
    b.close AS first_close,
    c.close AS last_close
FROM temp_start_end_dates a
JOIN historical_stock_prices_with_year b ON a.ticker = b.ticker AND a.first_date = b.data AND a.year = YEAR(b.data)
JOIN historical_stock_prices_with_year c ON a.ticker = c.ticker AND a.last_date = c.data AND a.year = YEAR(c.data);

-- Creazione della tabella 'grouped_values' con i valori raggruppati per ticker, nome e anno
CREATE TABLE grouped_values AS
    SELECT
        a.ticker,
        a.name,
        a.year,
        ROUND(((b.last_close - b.first_close) / b.first_close) * 100, 1) AS price_change_pct,
        MIN(low) AS min_price,
        MAX(high) as max_price,
        ROUND(AVG(volume),1) AS avg_volume
    FROM historical_stock_prices_with_year a
    JOIN temp_first_last_close b ON a.ticker = b.ticker AND a.year = b.year
    GROUP BY a.ticker, a.name, a.year, b.last_close, b.first_close
    ORDER BY ticker, name, year;

-- Creazione della tabella 'final' con i valori finali concatenati e ordinati per ticker
CREATE TABLE final AS
  SELECT
    ticker,
    name,
    CONCAT_WS(' ', COLLECT_LIST(CONCAT('(',
        CAST(year AS STRING), ', ',
        CAST(price_change_pct AS STRING), ', ',
        CAST(min_price AS STRING), ', ',
        CAST(max_price AS STRING), ', ',
        CAST(avg_volume AS STRING), ')'
    ))) AS concatenated_values
  FROM grouped_values
  GROUP BY ticker, name
ORDER BY ticker;

select * from final limit 10;