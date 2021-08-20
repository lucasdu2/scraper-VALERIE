# gross_spider.py 
# scraper does following things:
#   1: parses through csv file to obtain list of indexed URLs and
#      populates sqlite3 db with csv data
#   2: loops through list of URLs with scrapy
#   3: uses scrapy to search docs linked to URLs for case-insensitive 
#      keywords and related data according to Valerie's instructions
#   4: adds data into sqlite3 db
#   5: converts sqlite3 db into csv file
# IN: csv file 
# OUT: csv file with scraped data 

import csv, sqlite3
import scrapy
from w3lib.html import remove_tags
import re

# create list to hold list of URLs
#urlList = []
# create sqlite3 db and connection to db
#conn = sqlite3.connect(r"k8_url1_sqlite.db")
#cursor = conn.cursor()
#cursor.execute("""CREATE TABLE IF NOT EXISTS k8 (
#                    cik,
#                    coname,
#                    rdate,
#                    gvkey,
#                    URL,
#                    rdq,
#                    gm_d,
#                    gm_a,
#                    gm_f,
#                    gm_g,
#                    gm_ag,
#                    gf_d,
#                    gf_a,
#                    gf_f,
#                    gf_g,
#                    gf_ag
#                );""")
# parse through csv and obtain list of URLs
#with open ('k8_url1.csv') as csv_file:
#   csv_reader = csv.reader(csv_file, delimiter=',')
#   # skip header row
#   next(csv_reader)
#   # loop through csv row by row and fill in urlList
#   # also populate sqlite3 db
#   for row in csv_reader:
#       if row[4] != '':
#           # strip leading/trailing spaces from URLs to be
#           # passed to parse
#           urlNoSpace = row[4].strip()
#           urlList.append(urlNoSpace)
            # insert row into sqlite3 db
#           sql_row = (row[0], row[1], row[2], row[3], row[4], row[5])
#           cursor.execute("""INSERT INTO k8(cik, coname, rdate, gvkey, URL, rdq) 
#                              VALUES(?,?,?,?,?,?)""", sql_row)
## commit changes to sqlite3 db 
#conn.commit()
## close connection to sqlite3 db
#conn.close()

#print(urlList)
## TEST: loop prints through urlList
#counter = 0
#for url in urlList:
#    print(f'{counter}: {url}')
#    counter += 1


# create item class to hold relevant data for each doc 
class Data(scrapy.Item):
    # rowid of document
    rowid = scrapy.Field()
    # data fields needed for each document
    gm_d = scrapy.Field()
    gm_a = scrapy.Field()
    gm_f = scrapy.Field()
    gm_g = scrapy.Field()
    gm_ag = scrapy.Field()
    gf_d = scrapy.Field()
    gf_a = scrapy.Field()
    gf_f = scrapy.Field()
    gf_g = scrapy.Field()
    gf_ag = scrapy.Field()

# create spider to loop through list of URLs with scrapy and
# find/format/output relevant data
class GrossSpider(scrapy.Spider):
    name = 'gross_spider_SINGLE'
    start_urls = [
    'https://www.sec.gov/Archives/edgar/data/4515/0000004515-12-000017-index.htm'
    ]

    def parse(self, response):
        # open connection to k8 db
        conn = sqlite3.connect(r"k8_url1_sqlite.db")
        cursor = conn.cursor()
        # find rowid for current url
        request_url = response.request.url
        cursor.execute("""SELECT rowid FROM k8 WHERE URL = ?""", [request_url]) 
        rowid = cursor.fetchone()[0]
        # close connection to k8 db
        conn.close()

        # get press release link
        all_links = response.css('td a::attr(href)').getall()   
        # loop through link list until we get a valid link
        link_cnt = 1
        get_page = all_links[link_cnt]
        page_file = get_page[-4:]
        while (page_file != '.htm' and page_file != '.txt'):
            link_cnt += 1
            get_page = all_links[link_cnt]
            page_file = get_page[-4:]
        # pass rowid as metadata to parse_report
        if get_page is not None:
            yield response.follow(get_page, callback=self.parse_report, 
                            meta = {'rowid': rowid})
        else:
            pass

    def parse_report(self, response):
        # create rowid variable in parse_report
        rowid = response.meta['rowid']
        # create Data item instance to store data
        # store rowid in Data item
        data = Data(rowid = rowid)
        # initialize all other fields

    # decode byte object to string
        bodyDecoded = (response.body).decode('utf-8')
        # remove HTML tags
        bodyNoTags = remove_tags(bodyDecoded)
        # replace HTML for various Unicode symbols with string spaces
        bodyClean = re.sub(r'&[\w#]{4,5};', r' ', bodyNoTags) 
        # create new string with single space separation b/w words
        bodyFinal = ' '.join(bodyClean.split())

        kw_grossMargin = [' gross margin', ' gross profit margin', ' Gross Margin', 
                        ' Gross margin', ' Gross Profit Margin', ' Gross profit margin',]
        kw_grossProfit = [' gross profit', ' Gross profit', ' Gross Profit',]
        kw_guidance = ['guidance', 'expectation', 'forecast', 'expect',
                       'Guidance', 'Expectation', 'Forecast', 'Expect',]
    
        # initialize lists for keyword searches
        gm_found = []
        gf_found = []

        # search for gross margin keywords
        for kw in kw_grossMargin:
            gm_found += re.finditer(kw, bodyFinal)
        if len(gm_found) != 0:
            #print("GROSS MARGINS FOUND!\n")
            print(gm_found)
            data['gm_d'] = 1
            data['gm_f'] = len(gm_found)
        else: 
            #print("NO GROSS MARGINS FOUND!\n")
            data['gm_d'] = 0
            data['gm_f'] = 0
        
        data['gm_g'] = 0
        for m in gm_found:
            start = m.start()
            end = m.end()
            # search around each gross margin keyword for guidance keywords
            sliceStart = max(start - 50, 0)
            sliceEnd = min(end + 50, len(bodyFinal))
            bodyFinal_slice = bodyFinal[sliceStart: sliceEnd]
            if any(kw in bodyFinal_slice for kw in kw_guidance):
                data['gm_g'] = 1    
            # remove instances of gross margin kw from bodyFinal
            # to prevent gross profit search from finding these kw
            bodyFinal = bodyFinal[:start] + " " + bodyFinal[end + 1:]

        # search for gross profit keywords
        for kw in kw_grossProfit:
            gf_found += re.finditer(kw, bodyFinal)
        if len(gf_found) != 0:
            data['gf_d'] = 1
            data['gf_f'] = len(gf_found)
        else:
            data['gf_d'] = 0
            data['gf_f'] = 0

        data['gf_g'] = 0
        for m in gf_found:
            start = m.start()
            end = m.end()
            # search around each gross margin keyword for guidance keywords
            sliceStart = max(start - 50, 0)
            sliceEnd = min(end + 50, len(bodyFinal))
            bodyFinal_slice = bodyFinal[sliceStart: sliceEnd]
            if any(kw in bodyFinal_slice for kw in kw_guidance):
                data['gf_g'] = 1    
        print(data)
        yield data  
