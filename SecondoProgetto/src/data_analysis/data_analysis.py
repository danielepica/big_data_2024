import pandas as pd

csv_input_file1 = '/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/original_data/historical_stock_prices.csv'
csv_input_file2 = '/Users/danielepica/Desktop/Big_Data/Progetto_uguale_per_tutti/SecondoProgetto/data/original_data/historical_stocks.csv'

df1 = pd.read_csv(csv_input_file1, encoding='utf-8')
df2 = pd.read_csv(csv_input_file2, encoding='utf-8')


def get_statistics(df):
    #Get general information about dataset
    print(df.info())

    # Descriptive statistics
    print(df.describe())

    num_entries = len(df)
    # Stampa il numero di tuple nel file JSON
    print("Numero di tuple nel file JSON:", num_entries)

    #Unique value for ticker
    ticker_unique = df['ticker'].unique()
    print(f"There are {len(ticker_unique)} unique tickers.")

    # Verify empty record for column
    empty_records_per_column = df.isnull().sum()
    print("Number of empty records per column:")
    print(empty_records_per_column)


get_statistics(df1)
get_statistics(df2)

def get_difference_ticker(df1, df2):

    # Trova i valori di 'ticker' presenti in df2 ma non in df1
    valori_diversi = df2[~df2['ticker'].isin(df1['ticker'])]

    # Stampa i valori diversi
    print("Valori diversi: ")
    print(valori_diversi)
    print(f"In totale sono: {len(valori_diversi)}")
    return valori_diversi['ticker'].tolist()

get_difference_ticker(df1,df2)