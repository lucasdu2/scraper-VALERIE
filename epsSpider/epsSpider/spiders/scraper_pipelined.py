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

# parse through csv and obtain list of URLs
with open ('test1.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    # create list to hold list of URLs
    urlList = []
    with open('test1_noEmptyURL.csv', 'w+') as csv_new:
        csv_writer = csv.writer(csv_new, delimiter=',')
        # handle header row in csv
        headerRow = next(csv_reader)       
        csv_writer.writerow(headerRow)
        # loop through csv row by row, remove rows with empty URLs
        # and fill in urlList/create new csv file
        for row in csv_reader:
            if row[6] != '':
                # strip leading/trailing spaces from URLs to be
                # passed to parse
                urlNoSpace = row[6].strip()
                urlList.append(urlNoSpace)
                csv_writer.writerow(row)
#    print(urlList)
#    # TEST: loop prints through urlList
#    counter = 0
#    for url in urlList:
#        print(f'{counter}: {url}')
#        counter += 1

# create item class to hold relevant data for each doc 
class EPSData(scrapy.Item):
    isIn = scrapy.Field()
    strings = scrapy.Field()

# create spider to loop through list of URLs with scrapy and
# find/format/output relevant data
class EPSSpider(scrapy.Spider):
    name = 'eps_pipelined'

    def start_requests(self):
        urls = urlList
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
    
    # search for dollar values within a set # of characters around keyword
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
        else:
            self.parse_report(response)

    def parse_report(self, response):
        # decode byte object to string
        bodyDecoded = (response.body).decode('utf-8')
        # remove HTML tags
        bodyNoTags = remove_tags(bodyDecoded)
        # replace HTML for various Unicode symbols with string spaces
        bodyClean = re.sub(r'&[\w#]{4,5};', r' ', bodyNoTags) 
        # create new string with single space separation b/w words
        bodyFinal = ' '.join(bodyClean.split())

        # NOTE: ' eps.' is highly unlikely to appear 
        kwList = [' eps ', ' eps.', ' EPS ', ' EPS.', 'earnings per share', 'Earnings per share',
                  'earnings per diluted share', 'Earnings per diluted share',]
        kwStrings = []

        # search bodyFinal for keywords and return relevant data in a tuple
        if any(kw in bodyFinal for kw in kwList):
            keywordIn = 1
            for kw in kwList:
                kwStartEnd = ((m.start(), m.end()) for m in re.finditer(kw, bodyFinal))
                for (i, j) in kwStartEnd:
                    self.valueSearch(i, j, kwStrings, bodyFinal)
        else:
            keywordIn = 0
        
        # creates tuple for all relevant data in document
        epsData = EPSData(isIn = keywordIn, strings = kwStrings)
        yield epsData
