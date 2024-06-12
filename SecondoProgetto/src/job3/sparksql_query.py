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
df = df.filter((col("industry").isNotNull()) & (col("year") >= 2000))

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
industry_change_df.createOrReplaceTempView("industry_change")

# Calculate variations per year
year_variation_query = """
SELECT
    year,
    industry_change_pct,
    COLLECT_LIST(industry) AS industries
FROM industry_change
GROUP BY year, industry_change_pct
HAVING SIZE(industries) > 1
ORDER BY year, industry_change_pct
"""

year_variation_df = spark.sql(year_variation_query)
year_variation_df.createOrReplaceTempView("year_variation")

# Aggregate by year
year_map_query = """
SELECT
    year,
    COLLECT_LIST(NAMED_STRUCT('variation', industry_change_pct, 'industries', industries)) AS variations
FROM year_variation
GROUP BY year
ORDER BY year
"""

year_map_df = spark.sql(year_map_query)
year_map_dict = {row['year']: row['variations'] for row in year_map_df.collect()}

# Function to find intersections for a year
def find_intersections_for_year(data, year, min_years=3):
    intersections = []

    current_year_data = data.get(year, [])
    for entry in current_year_data:
        variation1 = entry['variation']
        companies1 = entry['industries']
        consecutive_years = [(year, variation1)]
        common_companies = set(companies1)
        consecutive_count = 1

        for next_year in range(year + 1, year + min_years):
            next_year_data = data.get(next_year, [])
            found_in_year = []
            for next_entry in next_year_data:
                variation2 = next_entry['variation']
                companies2 = next_entry['industries']
                new_common_companies = common_companies & set(companies2)
                if new_common_companies and len(new_common_companies) > 1:
                    found_in_year.append((next_year, variation2, new_common_companies))

            if not found_in_year:
                break

            new_consecutive_years = []
            for next_year, variation2, new_common_companies in found_in_year:
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
                for next_entry in next_year_data:
                    variation3 = next_entry['variation']
                    companies3 = next_entry['industries']
                    new_common_companies = common_companies & set(companies3)
                    if new_common_companies and len(new_common_companies) > 1:
                        found_in_year.append((additional_year, variation3, new_common_companies))

                if not found_in_year:
                    break

                new_consecutive_years = []
                for additional_year, variation3, new_common_companies in found_in_year:
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

# Function to remove duplicates
def remove_duplicates(intersections):
    final_intersections = []
    intersections.sort(key=lambda x: len(x[1]), reverse=True)

    for i, (companies1, years1) in enumerate(intersections):
        is_subset = False
        for j, (companies2, years2) in enumerate(intersections):
            if i != j and set(companies1).issubset(set(companies2)) and set(years1).issubset(set(years2)):
                is_subset = True
                break
        if not is_subset:
            final_intersections.append((companies1, years1))

    return final_intersections

# Find intersections for each year
results = []
for year in sorted(year_map_dict.keys()):
    results.extend(find_intersections_for_year(year_map_dict, year))

results = remove_duplicates(results)

# Print results
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

# Stop the SparkSession
spark.stop()

