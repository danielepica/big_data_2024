import pandas as pd
from src.data_analysis.data_analysis import get_difference_ticker

csv_input_1 = '/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/original_data/historical_stock_prices.csv'
csv_input_2 = '/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/original_data/historical_stocks.csv'

# Definisci una funzione per pulire i campi
def clean_field(field):
    # Rimuovi le virgolette iniziali e finali
    if isinstance(field, str):
        field = field.strip()
        field = field.strip('"')
        # Rimuovi le virgole all'interno del campo
        field = field.replace(',', '')
    return field

# Read the csv file in a dataframe
df1 = pd.read_csv(csv_input_1)
df2 = pd.read_csv(csv_input_2)

# Applica la funzione di pulizia al campo 'name' e industry che possono iniziare con " " e contenere ,
#df2['name'] = df2['name'].apply(clean_field)
#df2['industry'] = df2['industry'].apply(clean_field)

for field in df2.columns:
    df2[field] = df2[field].apply(clean_field)
for field in df1.columns:
    df1[field] = df1[field].apply(clean_field)

print("Initial length of unique value of ticker in historical_stocks.csv")
print(len(df2['ticker'].unique()))

# Remove a specific column
column_to_drop = 'adj_close'
df1 = df1.drop(columns=[column_to_drop])

df1_10k = df1.sample(n=10000)
df1_10k.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/10k_historical_stock_prices.csv', index=False)
df1_100k = df1.sample(n=100000)
df1_100k.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/100k_historical_stock_prices.csv', index=False)
df1_1000k = df1.sample(n=1000000)
df1_1000k.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/1000k_historical_stock_prices.csv', index=False)
df1.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/historical_stock_prices.csv', index=False)

# Remove tickers that are in df2 but not in df1 (are not relevant for our study)
to_remove = get_difference_ticker(df1, df2)
df2 = df2.loc[~df2['ticker'].isin(to_remove)]
print("Final length of unique value of ticker in historical_stocks.csv")
print(len(df2['ticker'].unique()))
df2.to_csv("/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/historical_stocks.csv", index=False)

# Unisci i dataset usando la colonna 'ticker' come chiave
merged_df = pd.merge(df1, df2[['ticker', 'name', 'sector', 'industry']], on='ticker', how='left')
# Seleziona 1000 righe casuali
df_sample = merged_df.sample(n=1000)
df_sample1 = merged_df.sample(n=10000)
df_sample2 = merged_df.sample(n=100000)
df_sample3 = merged_df.sample(n=1000000)

# Save new dataframes in csv files
merged_df.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/historical_stock_prices_merged.csv', index=False)
df_sample.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/1k_historical_stock_prices_merged.csv', index=False)
df_sample1.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/10k_historical_stock_prices_merged.csv', index=False)
df_sample2.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/100k_historical_stock_prices_merged.csv', index=False)
df_sample3.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/data_modified/1000k_historical_stock_prices_merged.csv', index=False)


#df2.to_csv('/Users/danielepica/Desktop/Big_Data/Progetto uguale per tutti/SecondoProgetto/data/data_modified/historical_stocks.csv', index=False)


