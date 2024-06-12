#!/usr/bin/env python3
"""mapper.py"""

import sys

line_count = 0
# read lines from STDIN (standard input)
for line in sys.stdin:
    # Incrementa il contatore
    line_count += 1

    # Salta la prima riga
    if line_count == 1:
        continue

    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into words
    words = line.split(",")
    ticker = words[0]
    open = words[1]
    close = words[2]
    low = words[3]
    high = words[4]
    volume = words[5]
    date = words[6]
    name = words[7]
    sector = words[8]
    industry = words[9]

    if sector == '':
        sector = 'NA'
    if industry == '':
        industry = 'NA'

    # write in standard output the mappings word -> 1
    # in the form of tab-separated pairs
    print('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (ticker, name, open, close, low, high, volume, date, industry))