from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
import argparse

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
arg = parser.parse_args()
input_filepath, output_filepath = arg.input_path, arg.output_path

# Initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Industry Report Job").getOrCreate()

# Load and split data
df = spark.read.option("header", "true").csv(input_filepath)

# Convert columns to appropriate types and create a temporary view
df = df.withColumn("open", col("open").cast("float")) \
       .withColumn("close", col("close").cast("float")) \
       .withColumn("volume", col("volume").cast("int")) \
       .withColumn("date", col("date").cast("date"))
# Add the year column
df = df.withColumn("year", year(col("date")))

df.createOrReplaceTempView("stocks")

# Perform SQL queries
result = spark.sql("""
WITH 
industry_data AS (
    SELECT
        industry,
        YEAR(date) AS year,
        SUM(open) AS open_sum,
        SUM(close) AS close_sum,
        SUM(volume) AS volume_sum,
        COUNT(*) AS count
    FROM stocks
    WHERE industry IS NOT NULL
    GROUP BY industry, YEAR(date)
),
industry_change AS (
    SELECT
        industry,
        year,
        ROUND(((close_sum - open_sum) / open_sum) * 100, 1) AS industry_change_pct,
        open_sum,
        close_sum,
        volume_sum,
        count
    FROM industry_data
),
ticker_data AS (
    SELECT
        industry,
        YEAR(date) AS year,
        ticker,
        SUM(open) AS open_sum,
        SUM(close) AS close_sum,
        SUM(volume) AS volume_sum,
        COUNT(*) AS count
    FROM stocks
    GROUP BY industry, YEAR(date), ticker
    ORDER BY volume_sum DESC
),
ticker_change AS (
    SELECT
        industry,
        year,
        ticker,
        ROUND(((close_sum - open_sum) / open_sum) * 100, 1) AS ticker_change_pct,
        open_sum,
        close_sum,
        volume_sum,
        count
    FROM ticker_data
    ORDER BY industry, year, ticker_change_pct DESC

),
max_ticker_change AS (
    SELECT
        industry,
        year,
        FIRST(ticker) AS max_ticker,
        MAX(ticker_change_pct) AS max_ticker_change_pct
    FROM ticker_change
    GROUP BY industry, year
    ORDER BY industry, year, max_ticker_change_pct DESC
),
max_ticker_volume AS (
    SELECT
        industry,
        year,
        FIRST(ticker) AS max_volume_ticker,
        MAX(volume_sum) AS max_volume_sum
    FROM ticker_data
    GROUP BY industry, year
    ORDER BY industry, year, max_volume_sum DESC
)
SELECT
    stocks.sector,
    industry_change.industry,
    industry_change.year,
    industry_change.industry_change_pct,
    max_ticker_change.max_ticker AS ticker_max_change_pct,
    max_ticker_change.max_ticker_change_pct AS max_ticker_change,
    max_ticker_volume.max_volume_ticker AS ticker_max_volume,
    max_ticker_volume.max_volume_sum AS max_volume
FROM 
    industry_change
JOIN 
    max_ticker_change ON industry_change.industry = max_ticker_change.industry AND industry_change.year = max_ticker_change.year
JOIN 
    max_ticker_volume ON industry_change.industry = max_ticker_volume.industry AND industry_change.year = max_ticker_volume.year
JOIN 
    (SELECT DISTINCT industry, year, sector FROM stocks WHERE industry IS NOT NULL) AS stocks ON industry_change.industry = stocks.industry AND industry_change.year = stocks.year
ORDER BY 
    stocks.sector, industry_change.industry, industry_change.industry_change_pct DESC
""")

# Save the final report
result.write.csv(output_filepath, header=True)

# Print the first 10 results
result.show(10, truncate= False)

# Stop the SparkSession
spark.stop()

