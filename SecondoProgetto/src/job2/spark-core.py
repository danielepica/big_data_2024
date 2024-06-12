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
spark = SparkSession.builder.appName("Industry Report Job").getOrCreate()
sc = spark.sparkContext

# Load and split data
lines_RDD = sc.textFile(input_filepath).cache()

# Split the header
header = lines_RDD.first()
data = lines_RDD.filter(lambda row: row != header).map(lambda row: row.split(","))

# Define column indexes
ticker_idx = header.split(",").index("ticker")
open_idx = header.split(",").index("open")
close_idx = header.split(",").index("close")
volume_idx = header.split(",").index("volume")
date_idx = header.split(",").index("date")
industry_idx = header.split(",").index("industry")
sector_idx = header.split(",").index("sector")

# Create a tuple with the required fields
data = (data.filter(lambda row: row[industry_idx] != "")
    .map(lambda row: (
    row[ticker_idx],
    float(row[open_idx]),
    float(row[close_idx]),
    int(row[volume_idx]),
    datetime.strptime(row[date_idx], "%Y-%m-%d").year,
    row[industry_idx],
    row[sector_idx]
)))

# Create namedtuples for easier handling
IndustryStats = namedtuple("IndustryStats", ["open_sum", "close_sum", "volume_sum", "count"])
TickerStats = namedtuple("TickerStats", ["open_sum", "close_sum", "volume_sum", "count"])

# Aggregate data by (industry, year) for industry-level statistics
industry_data = data.map(lambda x: ((x[5], x[4]), (x[1], x[2], x[3], 1))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])) \
                    .mapValues(lambda v: IndustryStats(v[0], v[1], v[2], v[3]))
#Per avere il settore creo ((industry, year), sector) quindi (industry, year) -> sector così da poter fare il join
industry_sector = data.map(lambda x : ((x[5], x[4]), x[6]))


# Calculate industry percentage change (variazione percentuale nell'anno del'industria)
industry_change = industry_data.mapValues(lambda stats: round((((stats.close_sum - stats.open_sum) / stats.open_sum) * 100),1))
# Ora ho differenza percentuale per ogni industry e anno
# Praticamente ho (industry, year) -> variazione percentuale

# Aggregate data by (industry, year, ticker) for ticker-level statistics
ticker_data = data.map(lambda x: ((x[5], x[4], x[0]), (x[1], x[2], x[3], 1))) \
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])) \
                  .mapValues(lambda v: TickerStats(v[0], v[1], v[2], v[3]))

# Calculate ticker percentage change (Variazione percentuale nell'anno dell'azione)
ticker_change = ticker_data.mapValues(lambda stats: round((((stats.close_sum - stats.open_sum) / stats.open_sum) * 100),1))
# Ora ho differenza percentuale per ogni industry, anno, e ticker (industry, year, ticker) -> variazione

# Find the ticker with the maximum percentage change for each (industry, year)
# Da (industry, year, ticker) (variazione_percentuale) diventa -> (industry, year),(ticker e variazione) ma prendendo solo il ticker con la variazione max
max_ticker_change = ticker_change.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1]))) \
                                 .reduceByKey(lambda a, b: a if a[1] > b[1] else b)
# Quindi diventa (industry, year) -> (ticker, variazione)
#(('SPECIALTY CHEMICALS', 2015), ('OBCI', -6))


# Find the ticker with the maximum volume for each (industry, year)
max_ticker_volume = ticker_data.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1].volume_sum))) \
                               .reduceByKey(lambda a, b: a if a[1] > b[1] else b)
# Quindi diventa (industry, year) -> (Azione, media volume maggiore)
#(('RESTAURANTS', 2011), ('DAVE', 13200))



# Join industry change with max ticker change and max ticker volume

# Join the industry change with the max ticker change and max volume ticker
industry_report = ((industry_change.join(max_ticker_change).join(max_ticker_volume).join(industry_sector))
                                 .map(lambda x: (x[1][1], x[0][0], x[0][1], x[1][0][0][0], x[1][0][0][1], x[1][0][1]))
                   )
# Apply distinct to remove duplicate entries
industry_report = industry_report.distinct()

# Funzione per creare una chiave di ordinamento composta
def sort_key(x):
    return (x[0], x[1], -x[3])  # ticker asc, year asc, price_change_pct desc

#Ordina i risultati utilizzando la chiave di ordinamento composta
industry_report = industry_report.sortBy(sort_key)

# Save the final report
industry_report.saveAsTextFile(output_filepath)

# Print the first 10 results
for record in industry_report.take(10):
    print(record)

# Stop the SparkContext

sc.stop()

