-- Creazione della tabella 'historical_stock_prices' con la definizione dei campi e delle proprietà della tabella
drop table historical_stock_prices;

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
LOAD DATA LOCAL INPATH 'job1/historical_stock_prices_merged.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- Crea una tabella con la variazione percentuale annuale per ogni industria
drop table industry_annual_percentage_change;
CREATE TABLE IF NOT EXISTS industry_annual_percentage_change AS
SELECT
    hsp.industry,
    YEAR(hsp.data) AS year,
    ROUND((SUM(hsp.close) - SUM(hsp.open)) / SUM(hsp.open) * 100, 2) AS percentage_change
FROM
    historical_stock_prices hsp
WHERE
    YEAR(hsp.data) >= 2000 AND hsp.industry != ''
GROUP BY
    hsp.industry, YEAR(hsp.data);

SET hive.strict.checks.cartesian.product = false;
SET hive.mapred.mode = nonstrict;

-- Crea una tabella di anni
drop table years;
CREATE TABLE IF NOT EXISTS Years (year INT);
INSERT INTO Years VALUES
    (2000), (2001), (2002), (2003), (2004), (2005), (2006), (2007), (2008),
    (2009), (2010), (2011), (2012), (2013), (2014), (2015), (2016), (2017), (2018);

-- Crea una tabella di aziende
drop table Companies;
CREATE TABLE IF NOT EXISTS Companies AS
SELECT DISTINCT industry
FROM industry_annual_percentage_change;

-- Crea tutte le combinazioni di aziende e anni
drop table AllCombinations;
CREATE TABLE IF NOT EXISTS AllCombinations AS
SELECT industry, year
FROM Companies CROSS JOIN Years;

-- Riempie le variazioni
drop table FilledVariations;
CREATE TABLE IF NOT EXISTS FilledVariations AS
SELECT
    ac.industry,
    ac.year,
    v.percentage_change
FROM
    AllCombinations ac
LEFT JOIN
    industry_annual_percentage_change v
ON
    ac.industry = v.industry AND ac.year = v.year;

-- Crea una tabella temporanea con i dati ordinati e senza valori null
drop table NonNullVariations;
CREATE TABLE IF NOT EXISTS NonNullVariations AS
SELECT
    industry,
    year,
    percentage_change
FROM
    FilledVariations
WHERE
    percentage_change IS NOT NULL;

-- Crea una tabella temporanea con i periodi di variazioni percentuali identiche
drop table ConsecutiveTrends;
CREATE TABLE IF NOT EXISTS ConsecutiveTrends AS
SELECT DISTINCT
    a.industry AS industry_a,
    b.industry AS industry_b,
    a.year,
    a.percentage_change
FROM
    NonNullVariations a
JOIN
    NonNullVariations b
ON
    a.year = b.year
    AND a.percentage_change = b.percentage_change
    AND a.industry <> b.industry
ORDER BY industry_a, industry_b, year;


drop table SavedTrendGroups;
CREATE TABLE IF NOT EXISTS SavedTrendGroups AS
    WITH TrendGroups AS (
    SELECT
        industry1,
        industry2,
        year1,
        percentage_change1,
        year2,
        percentage_change2,
        year3,
        percentage_change3
    FROM (
        SELECT
            t1.industry_a AS industry1,
            t2.industry_b AS industry2,
            t1.year AS year1,
            t1.percentage_change AS percentage_change1,
            t2.year AS year2,
            t2.percentage_change AS percentage_change2,
            t3.year AS year3,
            t3.percentage_change AS percentage_change3
            --,ROW_NUMBER() OVER (PARTITION BY t1.industry_a, t2.industry_b, t1.year, t1.percentage_change ORDER BY t1.year) AS rn
        FROM
            ConsecutiveTrends t1
            JOIN ConsecutiveTrends t2 ON t1.industry_a = t2.industry_a AND t1.industry_b = t2.industry_b AND t1.year + 1 = t2.year
            JOIN ConsecutiveTrends t3 ON t2.industry_a = t3.industry_a AND t2.industry_b = t3.industry_b AND t2.year + 1 = t3.year
        WHERE
            t1.year + 2 = t3.year
    ) AS Subquery
    --WHERE
        --rn = 1
)
SELECT
    industry1,
    industry2,
    year1,
    percentage_change1,
    year2,
    percentage_change2,
    year3,
    percentage_change3
FROM
    TrendGroups;
