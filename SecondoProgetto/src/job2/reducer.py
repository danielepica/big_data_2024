#!/usr/bin/env python3
"""reducer.py"""
import sys
from datetime import datetime
import time

start_time = time.time()

sector_to_info = {}
ticker_stats = {}
ticker_volume = {}
general_statistics = {}
# input comes from STDIN


def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return round((((end - start) / start) * 100),1)

for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    words = line.split("\t")
    industry = words[0]
    ticker = words[1]
    name = words[2]
    open = float(words[3])
    close = float(words[4])
    low = words[5]
    high = words[6]
    volume = float(words[7])
    year = int(words[8])
    sector = words[9]


    ''' Per ciascun industria e per ciascun anno:
    l’azione dell’industria che ha avuto il maggior incremento percentuale nell’anno (con indicazione dell’incremento)
     l’azione dell’industria che ha avuto il maggior volume di transazioni nell’anno (con indicazione del volume).'''
    # initialize words that were not seen before with 0
    if sector not in sector_to_info:
        sector_to_info[sector] = {}
        '''sector: -> industry: -> anno -> (open_sum, close_sum)'''

        ticker_stats[sector] = {}
        '''sector: -> industry: -> anno -> ticker -> (open_sum, close_sum)'''

        ticker_volume[sector] = {}
        '''sector: -> industry: -> anno -> ticker -> (volume, count)'''

    if industry not in sector_to_info[sector]:
        sector_to_info[sector][industry] = {}
        ticker_stats[sector][industry] = {}
        ticker_volume[sector][industry] = {}

    if year not in sector_to_info[sector][industry]:
        sector_to_info[sector][industry][year] = [open, close]
        ticker_stats[sector][industry][year] = {}
        ticker_volume[sector][industry][year] = {}

    else:
        statistics_year = sector_to_info[sector][industry][year]
        statistics_year[0] += open
        statistics_year[1] += close

    if ticker not in ticker_stats[sector][industry][year]:
        ticker_stats[sector][industry][year][ticker] = [open, close]
        ticker_volume[sector][industry][year][ticker] = [volume, 1]
    else:
        statistics_ticker = ticker_stats[sector][industry][year][ticker]
        statistics_ticker[0] += open
        statistics_ticker[1] += close
        statistics_volume = ticker_volume[sector][industry][year][ticker]
        statistics_volume[0] += volume
        statistics_volume[1] += 1


#Calcolo la variazione percentuale della quotazione dell'industria nell'anno
#Variazione percentuale arrotondata della somma delle azioni dell'azienda all'apertura e della somma delle azioni dell'azienda alla chiusura.
for sector in sector_to_info:
    for industry in sector_to_info[sector]:
        statistics_year = sector_to_info[sector][industry]
        for year in statistics_year:
            statistics_year[year] = calculate_percentage_change(statistics_year[year][0], statistics_year[year][1])
        sorted_statistics_year = dict(sorted(statistics_year.items(), key=lambda item: item[1], reverse=True))
        sector_to_info[sector][industry] = sorted_statistics_year
# Ora gli anni all'interno di sector_to_info[sector][industry] sono ordinati per percentage_change decrescente


# Calcolo il ticker che per ogni industry e anno ha avuto la variazione maggiore
for sector in ticker_stats:
    for industry in ticker_stats[sector]:
        statistics_year = ticker_stats[sector][industry]
        for year in statistics_year:
            ticker_info = statistics_year[year]
            for ticker in ticker_info:
                ticker_info[ticker] = calculate_percentage_change(ticker_info[ticker][0], ticker_info[ticker][1])
            sorted_ticker_stats = dict(sorted(ticker_info.items(), key=lambda item: item[1], reverse=True))
            ticker_stats[sector][industry][year] = sorted_ticker_stats

#Ora ho sector->industry->year->ticker->variazione. Devo prendere il primo ticker e il suo valore poichè ho una lista ordinata.

#Calcolo il ticker che per ogni industry e anno ha avuto il volume maggiore
for sector in ticker_volume:
    for industry in ticker_volume[sector]:
        statistics_year = ticker_volume[sector][industry]
        for year in statistics_year:
            ticker_info = statistics_year[year]
            for ticker in ticker_info:
                ticker_info[ticker] = ticker_info[ticker][0]/ticker_info[ticker][1]
            sorted_ticker_volume = (sorted(ticker_info.items(), key=lambda item: item[1], reverse=True))
            ticker_volume[sector][industry][year] = sorted_ticker_volume

# Ora ho sector->industry->year->ticker->volume. Devo prendere il primo ticker e il suo valore poichè ho una lista ordinata.

#Ora devo fare un merge.

# Unire i due dizionari

merged_dict = {}

for sector in sector_to_info:
        merged_dict[sector] = {}
        for industry in sector_to_info[sector]:
                merged_dict[sector][industry] = {}
                for year in sector_to_info[sector][industry]:
                    variation = sector_to_info[sector][industry][year]
                    ticker_map = ticker_stats[sector][industry][year]
                    ticker_map2 = ticker_volume[sector][industry][year]
                    ticker_max_increment_ticker = next(iter(ticker_stats[sector][industry][year]))
                    max_increment = ticker_map[ticker_max_increment_ticker]
                    ticker_max_volume = next(iter(ticker_volume[sector][industry][year]))

                    merged_dict[sector][industry][year] = {
                            'variation': variation,
                            'max_ticker': ticker_max_increment_ticker,
                            'max_increment': max_increment,
                            'ticker_max_volume': ticker_max_volume[0],
                            'max_volume': ticker_max_volume[1]
                    }

#Stampo tutto
count = 0;
for sector in merged_dict:
    if count < 10:
        print("Sector: %s\t" % (sector))
        for industry in merged_dict[sector]:
            if count < 10:
                print("     Name industry: %s\t" % (industry))
                for year in merged_dict[sector][industry]:
                    print(f"              {year}: {merged_dict[sector][industry][year]}")
                    count += 1
                    if count == 10:
                        break
