# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import csv

class EPSSpiderPipeline(object):
    def open_spider(self, spider):
        self.csv_input = open('test1_noEmptyURL.csv', 'r')
        self.csv_reader = csv.reader(self.csv_input, delimiter=',')
        # skip header row
        self.headerRow = next(self.csv_reader)

        self.csv_output = open('test1_noEmptyURL_DATA.csv', 'w+')
        self.csv_writer = csv.writer(self.csv_output, delimiter=',')
        self.csv_writer.writerow(self.headerRow)

    def close_spider(self, spider):
        self.csv_input.close()
        self.csv_input.close()
            
    def process_item(self, item, spider):
        # get current line from csv
        self.currentLine = next(self.csv_reader)
        # update current line with data from item
        self.currentLine[7] = item.get('isIn')
        self.currentLine[8] = item.get('strings')
        # write line to csv output file
        self.csv_writer.writerow(self.currentLine)
        return item
