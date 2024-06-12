#!/usr/bin/env python3
"""reducer.py"""
import sys
import time
from collections import defaultdict
from datetime import datetime

start_time = time.time()

industry_to_info = {}
general_statistics = {}
# input comes from STDIN


def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return round((((end - start) / start) * 100),1)

# Funzione per trovare sequenze di almeno 3 anni consecutivi con lo stesso andamento
def find_consecutive_years(common_years, values1, values2):
    consecutive_years = []  # array finale
    temp_years = []  # array temporaneo

    for k in range(len(common_years) - 2):
        # Se hanno almeno 3 anni uguali
        if (values1[common_years[k]] == values2[common_years[k]] and
                values1[common_years[k + 1]] == values2[common_years[k + 1]] and
                values1[common_years[k + 2]] == values2[common_years[k + 2]]):

            if not temp_years or common_years[k] == temp_years[-1]:  # verifico se è vuota temp_years o se l'ultimo elemento è uguale al primo, per continuare la sequenza
                temp_years.extend(
                    [(common_years[k], values1[common_years[k]]), (common_years[k + 1], values1[common_years[k + 1]]),
                     (common_years[k + 2], values1[common_years[k + 2]])]) #li aggiungo agli elementi temporanei
                temp_years = list(sorted(set(temp_years)))  # Rimuovere duplicati perchè riaggiungo 2 volte lo stesso elemento.
            else:  # Verifico se la grandezza è maggiore o uguale a 3
                if len(temp_years) >= 3:
                    consecutive_years.append(
                        temp_years)  # aggiungo i valori temporanei /temp_years) su quelli permamenti Consecutive_years)
                temp_years = [(common_years[k], values1[common_years[k]]),
                              (common_years[k + 1], values1[common_years[k + 1]]),
                              (common_years[k + 2], values1[common_years[k + 2]])]

    if len(temp_years) >= 3:  # Verifico se la grandezza è maggiore o uguale a 3
        consecutive_years.append(temp_years)

    return consecutive_years
#Qui devo trovare delle aziende che abbiano per almeno 3 anni gli stessi andamenti
#Potrei usare come chiave industry e year

def find_matching_trends(industry_to_info):
    from collections import defaultdict

    # Funzione per trovare sequenze di almeno 3 anni consecutivi con lo stesso andamento
    #common_years sono anni in comune consecutivi presenti per 2 industrie. values sarebbe la mappa year->valore

    industry_clusters = defaultdict(list)

    industries = list(industry_to_info.keys())
    num_industries = len(industries)

    for i in range(num_industries - 1):
        for j in range(i + 1, num_industries):
            industry1, industry2 = industries[i], industries[j] #confronto a coppie di industry
            years1, years2 = set(industry_to_info[industry1].keys()), set(industry_to_info[industry2].keys())  # Mi prendo gli anni presenti nelle industry
            common_years = sorted(years1 & years2) #unisco gli anni in comune ordinati

            if not common_years: #se non ci sono anni in comune vado avanti
                continue

            years1 = industry_to_info[industry1] #values1 contiene year -> valore
            years2 = industry_to_info[industry2] #values2 contiene year -> valore

            consecutive_years = find_consecutive_years(common_years, years1, years2)

            for seq in consecutive_years:
                key = tuple(seq)
                industry_clusters[key].extend([industry1, industry2])

    # Rimuovere duplicati all'interno di ogni cluster e ordinare le industrie
    for key in industry_clusters.keys():
        industry_clusters[key] = sorted(set(industry_clusters[key]))

    matching = [(tuple(cluster), key) for key, cluster in industry_clusters.items() if len(cluster) > 1]
    return matching



for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    words = line.split("\t")
    industry = words[0]
    sector = words[1]
    ticker = words[2]
    name = words[3]
    open = float(words[4])
    close = float(words[5])
    low = words[6]
    high = words[7]
    volume = float(words[8])
    year = int(words[9])

    percentage_change_industry = 0

    if industry not in industry_to_info:
        ''' industry -> { year : industry_quotation, percentage_change_industry}'''
        industry_to_info[industry] = {}

    if year not in industry_to_info[industry]:
        ''' year -> (industry_quotation, percentage_change_industry)'''
        industry_to_info[industry][year] = [open, close]
    else:
        statistics_year = industry_to_info[industry][year]
        statistics_year[0] += open
        statistics_year[1] += close

#Calcolo la variazione percentuale della quotazione dell'industria nell'anno
for industry in industry_to_info:
    statistics_year = industry_to_info[industry]
    for year in statistics_year:
        statistics_year[year] = calculate_percentage_change(statistics_year[year][0], statistics_year[year][1])
        sorted_statistics_year = dict(sorted(statistics_year.items(), reverse = True))  # Ordina per anno
        industry_to_info[industry] = sorted_statistics_year

''' for industry in industry_to_info:
    print(f"industry : {industry}")
    print(f"{industry_to_info[industry]}")'''

matching_trends = find_matching_trends(industry_to_info)

#for trend in matching_trends:
    #print(trend)

def format_matching_trends(matching_trends):
    formatted_results = []

    for cluster, years_values in matching_trends:
            # Converti le tuple delle industrie in una stringa
        industries_str = ", ".join(cluster)

            # Converti le tuple degli anni e valori in una stringa
        years_values_str = ", ".join([f"{year}:{value}%" for year, value in years_values])

            # Formatta il risultato finale
        formatted_result = f"{{{industries_str}}}: {years_values_str}"
        formatted_results.append(formatted_result)

    return formatted_results

formatted_result = format_matching_trends(matching_trends)

for trend in formatted_result:
      print(trend)

end_time = time.time()

# Calcolo del tempo di esecuzione totale
execution_time = end_time - start_time
print(f"Tempo di esecuzione reduce: {execution_time} secondi")