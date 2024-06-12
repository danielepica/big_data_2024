from pyspark import SparkContext
from pyspark.sql import SparkSession
import argparse
from datetime import datetime
from collections import namedtuple

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
arg = parser.parse_args()
input_filepath, output_filepath = arg.input_path, arg.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Job 1").getOrCreate()
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()
''' Il metodo .cache() in Apache Spark è utilizzato per memorizzare i dati in memoria (RAM) in modo 
che possano essere riutilizzati rapidamente nelle operazioni successive senza dover essere ricalcolati. 
Questo è particolarmente utile quando un RDD (Resilient Distributed Dataset) è usato più volte in una 
sequenza di trasformazioni e azioni.'''

# Splitta le righe in colonne
header = lines_RDD.first()
data = lines_RDD.filter(lambda row: row != header).map(lambda row: row.split(","))

# Definisci le posizioni delle colonne
ticker_idx = header.split(",").index("ticker")
date_idx = header.split(",").index("date")
close_idx = header.split(",").index("close")
low_idx = header.split(",").index("low")
high_idx = header.split(",").index("high")
volume_idx = header.split(",").index("volume")
name_idx = header.split(",").index("name")

# Crea un RDD di tuple con i campi necessari
data = data.map(lambda row: (
    row[ticker_idx],
    datetime.strptime(row[date_idx], "%Y-%m-%d"),
    float(row[close_idx]),
    float(row[low_idx]),
    float(row[high_idx]),
    int(row[volume_idx]),
    row[name_idx],
    datetime.strptime(row[date_idx], "%Y-%m-%d").year
))

# Defire una namedtuple per le statistiche annuali
AnnualStats = namedtuple("AnnualStats", ["first_close", "last_close", "first_date", "last_date", "min_price", "max_price", "total_volume", "count"])

# Funzione per creare una namedtuple con i valori iniziali
# Funzione per creare una namedtuple con i valori iniziali
def create_annual_stats(close: float, date: datetime, low: float, high: float, volume: float):
    return AnnualStats(close, close, date, date, low, high, volume, 1)

# Funzione per aggregare due AnnualStats
def merge_annual_stats(stats1: AnnualStats, stats2:AnnualStats):
    if stats1.first_date <= stats2.first_date:
        first_close, first_date = (stats1.first_close, stats1.first_date)
    else:
        first_close, first_date = (stats2.first_close, stats2.first_date)
    if stats1.last_date >= stats2.last_date:
        last_close, last_date = (stats1.last_close, stats1.last_date)
    else:
        last_close, last_date = (stats2.last_close, stats2.last_date)
    somma = stats1.count + stats2.count

    return AnnualStats(
        first_close,
        last_close,
        first_date,
        last_date,
        min(stats1.min_price, stats2.min_price),
        max(stats1.max_price, stats2.max_price),
        stats1.total_volume + stats2.total_volume,
        somma
    )

# Funzione per calcolare le statistiche finali
def calculate_final_stats(ticker, name, year, stats):
    price_change_pct = ((stats.last_close - stats.first_close) / stats.first_close) * 100
    avg_volume = round((stats.total_volume / stats.count),1)
    return ((ticker, name), (year, round(price_change_pct, 1), stats.min_price, stats.max_price, avg_volume))

# Raggruppa i dati per ticker, nome e anno
grouped_data = data.map(lambda x: ((x[0], x[6], x[7]), (create_annual_stats(x[2], x[1], x[3], x[4], x[5]))))
# (ticker, name, year) -> (close, date, low, high, volume)

# Aggrega i dati usando reduceByKey
aggregated_data = grouped_data.reduceByKey(lambda a, b : merge_annual_stats(a, b))


# Calcola le statistiche finali
final_stats = aggregated_data.map(lambda x: calculate_final_stats(x[0][0], x[0][1], x[0][2], x[1]))

# Ordina i risultati per nome e anno
sorted_final_stats = final_stats.sortBy(lambda x: (x[0][0], x[1][0]))

#Da  qui c'è più la chiave A, ma non è la prima
# Raggruppa i dati finali per chiave
grouped_final_stats = sorted_final_stats.groupByKey()

#print(grouped_final_stats.keys().collect())
# Trasforma ciascun gruppo in una lista di tuple
formatted_final_stats = grouped_final_stats.mapValues(lambda x: list(x))

formatted_final_stats.sortBy(lambda x: (x[0][0]))
# Per vedere tutte le chiavi
all_keys = grouped_final_stats.keys().collect()
print("Tutte le chiavi:", all_keys)

# Per cercare una chiave particolare
specific_key = ('A', 'AGILENT TECHNOLOGIES INC.')
specific_key_data = formatted_final_stats.filter(lambda x: x[0] == specific_key).collect()

print(f"Dati per la chiave {specific_key}: {specific_key_data}")

# Stampa l'output
#for key, value in formatted_final_stats.take(10):
 #   print(key, value)

