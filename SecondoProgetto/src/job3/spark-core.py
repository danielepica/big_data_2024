from pyspark import SparkContext
from pyspark.sql import SparkSession
import argparse
from datetime import datetime
from collections import namedtuple, defaultdict
import textwrap


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
arg = parser.parse_args()
input_filepath, output_filepath = arg.input_path, arg.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Job 3").getOrCreate()
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
data = (data.filter(lambda row: row[industry_idx] != "" and datetime.strptime(row[date_idx], "%Y-%m-%d").year >= 2000)
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
IndustryStats = namedtuple("IndustryStats", ["open_sum", "close_sum"])

# Aggregate data by (industry, year) for industry-level statistics
industry_data = data.map(lambda x: ((x[5], x[4]), (x[1], x[2]))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                    .mapValues(lambda v: IndustryStats(v[0], v[1]))


# Calculate industry percentage change (variazione percentuale nell'anno del'industria)
industry_change = industry_data.mapValues(lambda stats: round((((stats.close_sum - stats.open_sum) / stats.open_sum) * 100), 1))
# Ora ho differenza percentuale per ogni industry e anno
# Praticamente ho (industry, year) -> variazione percentuale
#industry_change.sortBy(lambda x : (x[0][1], x[1]))
year_variation = industry_change.map(lambda x: ((x[0][1],x[1]), x[0][0])).groupByKey().sortByKey()
# Ora ho (year, variation) -> [industry1, industry2]
year_variation_map = year_variation.mapValues(list) #per fare in modo di poterli stampare
# Filtrare solo i trend con più di una industria
filtered_year_variation_map = year_variation_map.filter(lambda x: len(x[1]) > 1)

year_map = year_variation_map.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().sortByKey()
#qui ho come chiave anno e come valore una lista di tuple con variazione percentuale e lista di aziende che hanno quella variazione.
''' (2015, [(-6, ['MEDICAL/DENTAL INSTRUMENTS', 'SPECIALTY CHEMICALS']), 
    (-3, ['MAJOR CHEMICALsS', 'ELECTRICAL PRODUCTS']), 
    (-2, ['MEDICAL SPECIALITIES', 'COMPUTER SOFTWARE: PREPACKAGED SOFTWARE', 'SPECIALTY FOODS']), 
    (-1, ['RETAIL: BUILDING MATERIALS', 'OTHER SPECIALTY STORES', 'HOME FURNISHINGS', 'EDP SERVICES', 'BIOTECHNOLOGY: BIOLOGICAL PRODUCTS (NO DIAGNOSTIC SUBSTANCES)']), 
    (0, ['CONSUMER: GREETING CARDS', 'FARMING/SEEDS/MILLING', 'BANKS', 'PACKAGE GOODS/COSMETICS', 'HOMEBUILDING', 'NEWSPAPERS/MAGAZINES', 'MAJOR BANKS', 'OIL & GAS PRODUCTION', 'ELECTRONIC COMPONENTS', 'MAJOR PHARMACEUTICALS', 'PACKAGED FOODS', 'ENGINEERING & CONSTRUCTION', 'TELECOMMUNICATIONS EQUIPMENT', 'INDUSTRIAL MACHINERY/COMPONENTS', 'SAVINGS INSTITUTIONS', 'SEMICONDUCTORS', 'REAL ESTATE INVESTMENT TRUSTS', 'MARINE TRANSPORTATION', 'AUTO MANUFACTURING', 'AUTO PARTS:O.E.M.']), 
    (1, ['METAL FABRICATIONS', 'OTHER CONSUMER SERVICES']), 
    (2, ['ALUMINUM', 'NATURAL GAS DISTRIBUTION']), 
    (4, ['CLOTHING/SHOE/ACCESSORY STORES']), 
    (5, ['MILITARY/GOVERNMENT/TECHNICAL'])])'''


def find_intersections_for_year(data, year, min_years=3):
    intersections = []

    current_year_data = data.get(year, [])
    for (variation1, companies1) in current_year_data:
        consecutive_years = [(year, variation1)]
        common_companies = set(companies1)
        consecutive_count = 1  # Contatore per tenere traccia degli anni consecutivi

        # Accumuliamo tutte le possibili intersezioni
        for next_year in range(year + 1, year + min_years):
            next_year_data = data.get(next_year, [])
            found_in_year = []
            for (variation2, companies2) in next_year_data:
                new_common_companies = common_companies & set(companies2)
                if new_common_companies and len(new_common_companies) > 1:
                    found_in_year.append((next_year, variation2, new_common_companies))

            if not found_in_year:
                break

            # Se troviamo più intersezioni, accumuliamo i risultati
            new_consecutive_years = []
            for (next_year, variation2, new_common_companies) in found_in_year:
                new_common_set = common_companies & new_common_companies
                if new_common_set and len(new_common_set) > 1:
                    new_consecutive_years.append((next_year, variation2, new_common_set))

            if new_consecutive_years:
                next_year_found = new_consecutive_years[0]
                common_companies = next_year_found[2]
                consecutive_years.append((next_year_found[0], next_year_found[1]))
                consecutive_count += 1
            else:
                break

        if len(common_companies) >= 2:
            for additional_year in range(year + min_years, max(data.keys()) + 1):
                next_year_data = data.get(additional_year, [])
                found_in_year = []
                for (variation3, companies3) in next_year_data:
                    new_common_companies = common_companies & set(companies3)
                    if new_common_companies and len(new_common_companies) > 1:
                        found_in_year.append((additional_year, variation3, new_common_companies))

                if not found_in_year:
                    break

                new_consecutive_years = []
                for (additional_year, variation3, new_common_companies) in found_in_year:
                    new_common_set = common_companies & new_common_companies
                    if new_common_set and len(new_common_set) > 1:
                        new_consecutive_years.append((additional_year, variation3, new_common_set))

                if new_consecutive_years:
                    additional_year_found = new_consecutive_years[0]
                    common_companies = additional_year_found[2]
                    consecutive_years.append((additional_year_found[0], additional_year_found[1]))
                else:
                    break

        if consecutive_count >= min_years and len(common_companies) >= 2:
            intersections.append((list(common_companies), consecutive_years))

    return intersections

def remove_duplicates(intersections):
    final_intersections = []
    intersections.sort(key=lambda x: len(x[1]), reverse=True)  # Ordina per lunghezza decrescente

    for i, (companies1, years1) in enumerate(intersections):
        is_subset = False
        for j, (companies2, years2) in enumerate(intersections):
            if i != j and set(companies1).issubset(set(companies2)) and set(years1).issubset(set(years2)):
                is_subset = True
                break
        if not is_subset:
            final_intersections.append((companies1, years1))

    return final_intersections


# Trasformare l'RDD in un dizionario per un facile accesso
data_dict = dict(year_map.collect())

# Trovare le intersezioni per ogni anno
results = []
for year in sorted(data_dict.keys()):
    results.extend(find_intersections_for_year(data_dict, year))

results = remove_duplicates(results)

# Stampare i risultati
def print_results(results):
    for companies, trends in results:
        company_str = ", ".join(companies)
        trends_str = ", ".join([f"{year}: {variation}" for year, variation in trends])
        wrapped_company_str = textwrap.fill(company_str, width=100)
        wrapped_trends_str = textwrap.fill(trends_str, width=100)
        try:
            print(f"[{wrapped_company_str}]: [{wrapped_trends_str}]")
        except Exception as e:
            print(f"Error printing results: {e}")
print_results(results)


# Metodo per trovare intersezioni di trend per anni consecutivi Ma esattamente 3 anni
'''
def find_consecutive_year_intersections(rdd_by_year):
    # Trova anni presenti nel dataset
    years = rdd_by_year.keys().distinct().collect()

    # Per ogni anno, trovare l'intersezione con gli anni successivi
    def find_intersections_for_year(year):
        current_year_data = rdd_by_year.filter(lambda x: x[0] == year).collect()
        next_year_data = rdd_by_year.filter(lambda x: x[0] == year + 1).collect()
        next_next_year_data = rdd_by_year.filter(lambda x: x[0] == year + 2).collect()

        intersections = []

        for (_, current_values) in current_year_data:
            for (variation1, companies1) in current_values:
                for (_, next_values) in next_year_data:
                    for (variation2, companies2) in next_values:
                        common_companies = list(set(companies1).intersection(companies2))
                        if common_companies and len(common_companies) > 1:
                            for (_, next_next_values) in next_next_year_data:
                                for (variation3, companies3) in next_next_values:
                                    final_common_companies = list(set(common_companies).intersection(companies3))
                                    if final_common_companies and len(final_common_companies) > 1:
                                        intersections.append((final_common_companies, [(year, variation1), (year + 1, variation2), (year + 2, variation3)]))
                        elif common_companies and len(common_companies) > 1:
                            intersections.append((common_companies, [(year, variation1), (year + 1, variation2)]))
        return intersections

    # Applica la funzione a tutti gli anni e raccogli i risultati
    result = []
    for year in years:
        result.extend(find_intersections_for_year(year))

    return result

# Applicare il metodo ai dati di input
result = find_consecutive_year_intersections(year_map)

# Stampare i risultati
for companies, trends in result:
    print(f"{companies}: {trends}")

#new_year_map = year_map.mapValues(list).collect()
#for row in new_year_map:
#    print(row)
#intersections = year_variation_map.flatMap(lambda x: [(x[0][0] + 1, (x[0][1], y)) for y in x[1] if x[0][1] < 0]).collect()


#Trasforma i risultati in lista
#industry_trends_list = industry_trends.mapValues(list).collect()




Risultato ora è qualcosa del tipo:
CATALOG/SPECIALTY DISTRIBUTION: [(2000, 7), (2014, 0)]
CONSUMER ELECTRONICS/APPLIANCES: [(2004, 0)]
RAILROADS: [(2008, -4)]
OILFIELD SERVICES/EQUIPMENT: [(2014, -4)]
DEPARTMENT/SPECIALTY RETAIL STORES: [(2002, 2)]
'''


#data = industry_trends.collect()
#for row in data:
 #   print(row)



# Stop the SparkContext

sc.stop()