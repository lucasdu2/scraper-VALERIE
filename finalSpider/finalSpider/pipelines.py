# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3

class FinalspiderPipeline(object):
	def process_item(self, item, spider):
		#print("IN PROCESS ITEM!!\n")
		c = sqlite3.connect(r"k8.db")
		cur = c.cursor()
		rowid = item.get('rowid')
		inputs = (item.get('gm_d'), item.get('gm_a'), item.get('gm_f'), item.get('gm_g'),
					item.get('gm_ag'), item.get('gf_d'), item.get('gf_a'), item.get('gf_f'),
					item.get('gf_g'), item.get('gf_ag'), rowid)
		cur.execute("""UPDATE k8 
					SET gm_d=?, gm_a=?, gm_f=?, gm_g=?, gm_ag=?,
					gf_d=?, gf_a=?, gf_f=?, gf_g=?, gf_ag=?
					WHERE rowid=?""", inputs)
		#print("finished updating\n")
		c.commit()
		c.close()
		return item
