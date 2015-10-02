# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import log
from twisted.enterprise import adbapi
from scrapy.http import Request
from scrapy.selector import Selector

import MySQLdb
import MySQLdb.cursors
import os
import time
import re

import sys
reload(sys)
sys.setdefaultencoding('utf8')

class TiantianPipeline(object):
	def __init__(self):
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			db = 'duang',
			user = 'duang',
			passwd = 'duang',
			cursorclass = MySQLdb.cursors.DictCursor,
			charset = 'utf8',
			use_unicode = False
		)

	def process_item(self, item, spider):
		query = self.dbpool.runInteraction(self._conditional_insert, item)
		query.addErrback(self.handle_error)
		return item

	def _conditional_insert(self, tx, item):
		tx.execute(\
				"insert into post (title, post_type, feed_type, content, updated, created) \
				values (%s, %s, %s, %s, %s, %s)",\
				(item['title'], 'live', '', '', item['date'], item['date']))
		post_id = tx.lastrowid

		signal_text = ''
		for signal in item['signal']:
			body = '<html><body>'+signal+'</body></html>'
			sel = Selector(text=body)
			video_name = sel.xpath('//a/text()').extract()[0]
			video_link = 'http://www.tiantian.tv'+sel.xpath('//a/@href').extract()[0]
			signal_text = video_name + ' ' + signal_text
			
			tx.execute(\
				"insert into video (name, link, vendor, open_type, created) \
				values (%s, %s, %s, %s, %s)",\
				(video_name, video_link, 'tiantian', 'new', item['date']))
			video_id = tx.lastrowid

			tx.execute(\
				"insert into object_video (video_id, obj_id, obj_type) \
				values (%s, %s, %s)",\
				(video_id, post_id, 'post'))
		
		tx.execute(\
				"insert into live (sport, game, team, signal_text, hot, post_id, date) \
				values (%s, %s, %s, %s, %s, %s, %s)",\
				(item['sport'], item['game'], item['team'], signal_text, item['hot'], post_id, item['date']))

	def handle_error(self, e):
		log.err(e)