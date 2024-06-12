import pandas as pd

# Leggi il file CSV
input_filepath = "data/data_modified/historical_stock_prices_merged.csv"
data = pd.read_csv(input_filepath)

# Raggruppa i dati per industry e conta i ticker unici
industry_ticker_count = data.groupby('industry')['ticker'].nunique().reset_index()

# Rinomina la colonna per chiarezza
industry_ticker_count.columns = ['industry', 'ticker_count']

# Salva i risultati in un file CSV
output_filepath = "industry_ticker_count.csv"
industry_ticker_count.to_csv(output_filepath, index=False)

# Stampa i primi 10 risultati
print(industry_ticker_count.head(10))
