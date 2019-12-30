import scrapy
from w3lib.html import remove_tags
import re

class TestSpider(scrapy.Spider):
    name = 'singleTest'
    start_urls = [
        'https://www.sec.gov/Archives/edgar/data/764622/0001047469-11-003290-index.htm'
    ]

    def valueSearch(self, start, end, kwStrings, text):
        # set search parameters for all cases 
        if (start-50 < 0) and (end+50 <= len(text)):
            beginSlice = 0; endSlice = end+50
        elif (start-50 >= 0) and (end+50 > len(text)):
            beginSlice = start-50; endSlice = len(text)
        elif (start-50 < 0) and (end+50 > len(text)):
            beginSlice = 0; endSlice = len(text)
        else:
            beginSlice = start-50; endSlice = end+50
        
        for c in text[beginSlice:endSlice]:
            if (c == '$'):
                kwStrings.append(text[beginSlice:endSlice])
                return

    def parse(self, response):
        # gets hyperlink to desired document ex-99.1 or ex-99.2
        get_page = response.css('td a::attr(href)').getall()[1]
        if get_page is not None:
            yield response.follow(get_page, callback=self.parse_report)
    
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
        if any(kw in bodyFinal for kw in kwList):
            keywordIn = 1
            kwStrings = []
            for kw in kwList:
                kwStartEnd = ((m.start(), m.end()) for m in re.finditer(kw, bodyFinal))
                for (i, j) in kwStartEnd:
                    self.valueSearch(i, j, kwStrings, bodyFinal)
        else:
            keywordIn = 0
        
        # creates tuple for all relevant data in document
        epsData = (keywordIn, kwStrings)
        print(epsData)
