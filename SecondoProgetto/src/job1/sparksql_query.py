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
#spark= SparkSession.builder.config("spark.driver.host", "localhost").appName("Task 1 Spark SQL").getOrCreate()

#su cluster uso
spark= SparkSession.builder.appName("Task 1 Spark SQL").getOrCreate()

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

#convertiamo il nostro DataFrame come una tabella SQL e diamo delle query SQL
df.createOrReplaceTempView("stocks")

final_df = spark.sql("""
WITH grouped_values AS (
    SELECT
        ticker,
        name,
        year,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, name, year ORDER BY date) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, name, year ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        FIRST_VALUE(date) OVER (PARTITION BY ticker, name, year ORDER BY date) AS first_date,
        LAST_VALUE(date) OVER (PARTITION BY ticker, name, year ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_date,
        MIN(low) OVER (PARTITION BY ticker, name, year) AS min_price,
        MAX(high) OVER(PARTITION BY ticker, name, year) AS max_price,
        AVG(volume) OVER (PARTITION BY ticker, name, year) AS avg_volume
    FROM stocks
),
final_values AS (
    SELECT
        ticker,
        name,
        year,
        ROUND(((last_close - first_close) / first_close) * 100, 1) AS price_change_pct,
        min_price,
        max_price,
        avg_volume
    FROM grouped_values
)
SELECT
    DISTINCT 
    ticker,
    name,
    year,
    CAST(price_change_pct AS FLOAT) AS price_change_pct,
    CAST(min_price AS FLOAT) AS min_price,
    CAST(max_price AS FLOAT) AS max_price,
    CAST(avg_volume AS FLOAT) AS avg_volume
FROM final_values
ORDER BY ticker, year
""")

# Save the output
#final_df.write.csv(output_filepath, header=True)

# Show the results
final_df.show(10)

# Stop the SparkSession
spark.stop()
