#!/usr/bin/env python3
"""reducer.py"""
import sys
from datetime import datetime
# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
ticker_to_info = {}
general_statistics = {}
# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    words = line.split("\t")
    ticker = words[0]
    name = words[1]
    open = words[2]
    close = words[3]
    low = words[4]
    high = words[5]
    volume = words[6]
    date = words[7]
    industry = words[8]

    # Converti la stringa in un oggetto data
    data = datetime.strptime(date, "%Y-%m-%d")

    # Estrarre l'anno
    anno = data.year


    # initialize words that were not seen before with 0
    if ticker not in ticker_to_info:
        statistics_year = {}
        ''' anno-> (data_minima_trovata_fin_ora, data_maxima_trovata_fin_ora, 
        close_data_minima, close_data_maxima, prezzo_minimo, prezzo_massimo, 
        volume, numero_date_per_calcolare_volume_medio)'''
        statistics_year[anno] = [date, date, close, close, low, high, [volume], 1]
        general_statistics[ticker] = statistics_year
        ticker_to_info[ticker] = [name, general_statistics[ticker]]
    else:
        statistics_year = general_statistics[ticker]
        if anno not in statistics_year:
            statistics_year[anno] = [date, date, close, close, low, high, [volume], 1]
        else:
            if statistics_year[anno][0] > date:
                statistics_year[anno][0] = date
                statistics_year[anno][2] = close
            elif statistics_year[anno][1] < date:
                statistics_year[anno][1] = date
                statistics_year[anno][3] = close
            if statistics_year[anno][4] > low:
                statistics_year[anno][4] = low
            if statistics_year[anno][5] < high:
                statistics_year[anno][5] = high
            statistics_year[anno][6].append(volume)
            statistics_year[anno][7] += 1

        ticker_to_info[ticker] = [name, general_statistics[ticker]]

''' la variazione percentuale della quotazione nell’anno (differenza percentuale
arrotondata tra il primo prezzo di chiusura e l’ultimo prezzo di chiusura dell’anno) → serve “close”

(ii) il prezzo minimo nell’anno, →  low

(iii) quello massimo nell’anno, → high

(iv) il volume medio dell’anno. → volume

per ora ho qualcosa del genere:
MCRI    ['"MONARCH CASINO & RESORT', 
['2005-08-30', '18.6299991607666', '18.3299999237061', '18.2800006866455', '18.7999992370605', '128700'], 
['2017-10-03', '40.2999992370605', '39.8199996948242', '39.6699981689453', '40.2999992370605', '55800']]'''

'''Io devo filtrare per date.
Posso creare una mappa chiave (anno) valore una lista che poi contiene anche la data effettiva
(mi servirà per poi fare la variazione percentuale della quotazione nell’anno).
Potrebbere essere:
anno (es:2010) -> (data_minima_trovata_fin_ora, data_maxima_trovata_fin_ora, close_data_minima, close_data_maxima, prezzo_minimo, prezzo_massimo, volume, numero_date_per_calcolare_volume_medio)
'''
for ticker in general_statistics:
    statistics_year = general_statistics[ticker]
    for anno in statistics_year:
        minium_date, max_date, first_close, last_close, low, high, volumes, n_volumes = statistics_year[anno]
        # Calcola la differenza percentuale
        differenza_percentuale = round((((float(last_close) - float(first_close)) / float(first_close)) * 100),1)
        # Arrotonda il risultato alla precisione desiderata (ad esempio, due decimali)
        percentage_variation = round(differenza_percentuale, 1)
        minimal_price = low
        maximal_price = high
        #Calcolo la media dei volumi
        lista_valori_numeri = [float(valore) for valore in volumes]
        somma_totale = sum(lista_valori_numeri)
        mean_volume = round((somma_totale/n_volumes),1)
        statistics_year[anno] = percentage_variation, minimal_price, maximal_price, mean_volume


for ticker in ticker_to_info:
    print("%s\t%s" % (ticker, ticker_to_info[ticker]))

''' MUX     ['MCEWEN MINING INC.', 
{1991: ['1991-01-07', '1991-09-24', '0.216000005602837', '0.0879999995231628', '0.0879999995231628', '0.216000005602837', 
['4000', '67000'], 2]}]'''

