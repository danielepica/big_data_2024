from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, min as spark_min, max as spark_max, avg, round
from pyspark.sql.window import Window
import argparse
from datetime import datetime

#creiamo il parser e settiamo gli argomenti
parser=argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

#memorizziamo gli argomenti dentro la variabile da dare all'RDD per la sua creazione
args= parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

#definiamo l' oggetto Spark di tipo SparkSession così da avere un punto di accesso unico per tutti gli altri moduli
spark= SparkSession.builder.config("spark.driver.host", "localhost").appName("Task 1 Spark SQL").getOrCreate()

#import the csv file as dataframe
df = spark.read.csv(input_filepath, header=True).cache()

# Convert columns to appropriate data types
df = df.withColumn("date", col("date").cast("date")) \
       .withColumn("open", col("open").cast("float")) \
       .withColumn("close", col("close").cast("float")) \
       .withColumn("low", col("low").cast("float")) \
       .withColumn("high", col("high").cast("float")) \
       .withColumn("volume", col("volume").cast("int")) \
       .withColumn("year", col("date").cast("string").substr(0, 4).cast("int"))

#df.show(5)
#df.printSchema()

# Define window specification
window_spec = Window.partitionBy("ticker", "name", "year").orderBy("date")

# Calculate the statistics
result_df = df.withColumn("first_close", first("close").over(window_spec)) \
              .withColumn("last_close", last("close").over(window_spec)) \
              .withColumn("first_date", first("date").over(window_spec)) \
              .withColumn("last_date", last("date").over(window_spec)) \
              .groupBy("ticker", "name", "year") \
              .agg(
                  first("first_close").alias("first_close"),
                  last("last_close").alias("last_close"),
                  first("first_date").alias("first_date"),
                  last("last_date").alias("last_date"),
                  spark_min("low").alias("min_price"),
                  spark_max("high").alias("max_price"),
                  avg("volume").alias("avg_volume")
              )
# Calculate percentage change
result_df = result_df.withColumn("price_change_pct", round(((col("last_close") - col("first_close")) / col("first_close")) * 100, 1))

# Select and sort the final columns
final_df = result_df.select(
    "ticker", "name", "year",
    col("price_change_pct").cast("float").alias("price_change_pct"),
    col("min_price").cast("float").alias("min_price"),
    col("max_price").cast("float").alias("max_price"),
    col("avg_volume").cast("float").alias("avg_volume")
).orderBy("name", "year")

# Save the output
final_df.write.csv(output_filepath, header=True)

# Show the results
final_df.show(10)

# Stop the SparkSession
spark.stop()