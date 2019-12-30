# scraper.py
# scraper does following things:
#   1: parses through csv file to obtain list of indexed URLs
#   2: loops through list of URLs with scrapy
#   3: uses scrapy to search docs linked to URLs for case-insensitive 
#   keywords: {eps, earnings per share, earnings per diluted share, etc.}
#   and scrape data associated with keywords (if keyword appears in doc, 
#   value of eps if given) 
#   4: adds data into original csv file 
# IN: csv file 
# OUT: csv file with scraped data 

import csv
import scrapy
from w3lib.html import remove_tags
import re

# TODO: TEMPORARY data structure to store all epsData tuples for each document
data = []

# PART 1: parse through csv and obtain list of URLs
with open ('test1.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    # create list to hold list of URLs
    urlList = []
    # skip header row in csv
    next(csv_reader)
    # loop through csv row by row and fill in urlList
    for row in csv_reader:
        urlNoSpace = row[6].strip()
        urlList.append(urlNoSpace)
#    print(urlList)
#    # TEST: loop prints through urlList
#    counter = 0
#    for url in urlList:
#        print(f'{counter}: {url}')
#        counter += 1

# TODO: export all data into csv file (see scraper_pipelined.py)
# PART 2: loop through list of URLs with scrapy
class EPSSpider(scrapy.Spider):
    name = 'eps'
    
    # list that stores all epsData tuples for each document
    data = []

    def start_requests(self):
        urls = urlList
        for url in urls:
            if url == '':
                data.append(('no link given', 'no link given'))
            else:
                yield scrapy.Request(url=url, callback=self.parse)

    def valueSearch(self, start, end, kwStrings, text):
        # set search parameters for all cases 
        if (start-64 < 0) and (end+64 <= len(text)):
            beginSlice = 0; endSlice = end+64
        elif (start-64 >= 0) and (end+64 > len(text)):
            beginSlice = start-64; endSlice = len(text)
        elif (start-64 < 0) and (end+64 > len(text)):
            beginSlice = 0; endSlice = len(text)
        else:
            beginSlice = start-64; endSlice = end+64

        for c in text[beginSlice:endSlice]:
            if (c == '$'):
                kwStrings.append(text[beginSlice:endSlice])
                return

    def parse(self, response):
        get_page = response.css('td a::attr(href)').get()
        if get_page is not None:
            yield response.follow(get_page, callback=self.parse_report)

    def parse_report(self, response):
        # decode byte object to string
        bodyDecoded = (response.body).decode('utf-8')
        # remove HTML tags
        bodyNoTags = remove_tags(bodyDecoded)
        # replace HTML for hard spaces with string spaces
        bodyClean = bodyNoTags.replace('&nbsp;', ' ')
        # create new string with single space separation b/w words
        bodyFinal = ' '.join(bodyClean.split())

        kwList = ['eps', 'EPS', 'earnings per share', 'Earnings per share',
                  'earnings per diluted share', 'Earnings per diluted share',]
        kwStrings = []
        if any(kw in bodyFinal for kw in kwList):
            keywordIn = 1
            for kw in kwList:
                kwStartEnd = ((m.start(), m.end()) for m in re.finditer(kw, bodyFinal))
                for (i, j) in kwStartEnd:
                    self.valueSearch(i, j, kwStrings, bodyFinal)
                    # TODO figure out why valueSearch is appending twice...
        else:
            keywordIn = 0
        
        # creates tuple for all relevant data in document
        epsData = (keywordIn, kwStrings)
        data.append(epsData)

