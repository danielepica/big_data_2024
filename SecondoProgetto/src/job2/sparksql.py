from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as spark_sum, avg, round, max as spark_max, first, desc, row_number
from pyspark.sql.window import Window

import argparse
from datetime import datetime

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
arg = parser.parse_args()
input_filepath, output_filepath = arg.input_path, arg.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Industry Report Job").getOrCreate()

# Load and split data
df = spark.read.option("header", "true").csv(input_filepath)

# Convert columns to appropriate types
df = df.withColumn("open", col("open").cast("float")) \
       .withColumn("close", col("close").cast("float")) \
       .withColumn("volume", col("volume").cast("int")) \
       .withColumn("date", col("date").cast("date"))

# Add a year column
df = df.withColumn("year", year(col("date")))

# Filter out rows with empty industry
df = df.filter(col("industry").isNotNull())

# Seleziona solo le colonne "sector", "industry" e "year"
industry_sector = df.select("sector", "industry", "year")

# Group by industry and year to calculate industry-level statistics
industry_data = df.groupBy("industry", "year") \
    .agg(
        spark_sum("open").alias("open_sum"),
        spark_sum("close").alias("close_sum"),
        spark_sum("volume").alias("volume_sum"),
        spark_sum("volume").alias("count")
    )

# Calculate industry percentage change
industry_change = industry_data.withColumn(
    "industry_change_pct",
    round(((col("close_sum") - col("open_sum")) / col("open_sum")) * 100, 1)
)

# Group by industry, year, and ticker to calculate ticker-level statistics (Il risultato è un nuovo dataframe)
ticker_data = df.groupBy("industry", "year", "ticker") \
    .agg(
        spark_sum("open").alias("open_sum"),
        spark_sum("close").alias("close_sum"),
        spark_sum("volume").alias("volume_sum"),
        spark_sum("volume").alias("count")
    )

# Calculate ticker percentage change
ticker_change = ticker_data.withColumn(
    "ticker_change_pct",
    round(((col("close_sum") - col("open_sum")) / col("open_sum")) * 100, 1)
)

# Find the ticker with the maximum percentage change for each (industry, year)
max_ticker_change = ticker_change \
    .orderBy(desc("ticker_change_pct")) \
    .groupBy("industry", "year") \
    .agg(
        first("ticker").alias("max_ticker"),
        spark_max("ticker_change_pct").alias("max_ticker_change_pct")
    )

# Find the ticker with the maximum volume for each (industry, year)
max_ticker_volume = ticker_data \
    .orderBy(desc("volume_sum")) \
    .groupBy("industry", "year") \
    .agg(
        first("ticker").alias("max_volume_ticker"),
        spark_max("volume_sum").alias("max_volume_sum")
    )


industry_report = industry_change.join(max_ticker_change, ["industry", "year"]) \
                                 .join(max_ticker_volume, ["industry", "year"]) \
                                 .join(industry_sector, ["industry", "year"])\
                                 .select(
                                  col("sector").alias("sector"),
                                     industry_change["industry"],
                                     industry_change["year"],
                                     industry_change["industry_change_pct"],
                                     col("max_ticker").alias("ticker_max_change_pct"),
                                     col("max_ticker_change_pct").alias("max_ticker_change"),
                                     col("max_volume_ticker").alias("ticker_max_volume"),
                                     col("max_volume_sum").alias("max_volume")
                                 ).orderBy("sector", "industry", desc("industry_change_pct"))

# Define a window for ranking
window_spec = Window.partitionBy("sector", "industry", "year").orderBy(desc("industry_change_pct"))

# Add a rank column to the data
industry_report = industry_report.withColumn("rank", row_number().over(window_spec))

# Filter to keep only the first ranked rows (remove duplicates)
result = industry_report.filter(col("rank") == 1).drop("rank")

# Order the final result
result = result.orderBy("sector", "industry", desc("industry_change_pct"))

# Save the final report
result.write.csv(output_filepath, header=True)

# Print the first 10 results
result.show(10)

spark.stop()
