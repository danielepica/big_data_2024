from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, round as spark_round
import argparse
import textwrap

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
arg = parser.parse_args()
input_filepath, output_filepath = arg.input_path, arg.output_path

# Initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Job 3").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv(input_filepath)

# Convert columns to appropriate types
df = df.withColumn("open", col("open").cast("float")) \
       .withColumn("close", col("close").cast("float")) \
       .withColumn("volume", col("volume").cast("int")) \
       .withColumn("date", col("date").cast("date")) \
       .withColumn("year", year(col("date")))

# Filter data for years >= 2000 and non-null industry
df = df.filter((col("industry") != '') & (col("year") >= 2000))

# Create a temporary view
df.createOrReplaceTempView("stocks")

# Calculate industry statistics
industry_data_query = """
WITH industry_data AS (
    SELECT
        industry,
        year,
        SUM(open) AS open_sum,
        SUM(close) AS close_sum
    FROM stocks
    GROUP BY industry, year
)
SELECT
    industry,
    year,
    ROUND(((close_sum - open_sum) / open_sum) * 100, 1) AS industry_change_pct
FROM industry_data
"""
industry_change_df = spark.sql(industry_data_query)
industry_change_df.createOrReplaceTempView("industries_change")

industry_change_df.show(truncate=False)
#industry_change_df.show()
# ora ho una nuova tabella con industry, year e variazione percentuale e year sono tutti >= 2000

query_year_variation = """
        SELECT 
            year,
            industry_change_pct,
            COLLECT_LIST(industry) AS industries
        FROM industries_change
        GROUP BY year, industry_change_pct
        HAVING COUNT(industry) > 1
          """

year_variation = spark.sql(query_year_variation)
year_variation.createOrReplaceTempView("year_variation")
year_variation.show(truncate=False)

# Step 1: Find all industries with same trend for at least 3 consecutive years
query = """
    SELECT 
        year,
        industry_change_pct,
        industries,
        LEAD(year, 1) OVER (ORDER BY year) AS next_year,
        LEAD(industry_change_pct, 1) OVER (ORDER BY year) AS next_change,
        LEAD(industries, 1) OVER (ORDER BY year) AS next_industries,
        LEAD(year, 2) OVER (ORDER BY year) AS next_next_year,
        LEAD(industry_change_pct, 2) OVER (ORDER BY year) AS next_next_change,
        LEAD(industries, 2) OVER (ORDER BY year) AS next_next_industries
    FROM year_variation
"""
prova = spark.sql(query)
prova.show()

'''consecutive_years_df = spark.sql(query)

# Output the results
consecutive_years_df.show(truncate=False)

# Save the results to the specified output path
consecutive_years_df.coalesce(1).write.csv(output_filepath, header=True)'''

# Stop the SparkSession
spark.stop()

