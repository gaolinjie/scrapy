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

dup = 0
DUP_THRESHOLD = 3


class ZbbPipeline(object):
	def __init__(self):
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			db = 'duang',
			user = 'duang',
			passwd = 'duang',
			cursorclass = MySQLdb.cursors.DictCursor,
			charset = 'utf8',
			use_unicode = False
		)
		dup = 0

	def process_item(self, item, spider):
		query = self.dbpool.runInteraction(self._conditional_insert, item)
		query.addErrback(self.handle_error)
		return item

	def _conditional_insert(self, tx, item):
		global dup
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')
		if item['post_type'] == 'video':
			title  = box.xpath('./div[@class="title"]/h1/text()').extract()
		content = box.xpath('./div[@class="content"]').extract()
		replys = box.xpath('./div[@class="hot_huifu"]').extract()
		created = box.xpath('./div[@class="title"]/span/text()').extract()
		created = created[0].split('  ')[0]

		tx.execute("select * from zbb where url=%s", (item['url'],))
		result=tx.fetchone()
		#log.msg(result, level=log.DEBUG)
		print result
		if result:
			#log.msg("Item already stored in db:%s" % item, level=log.DEBUG)
			log.msg("Item already stored in db", level=log.DEBUG)
			dup = dup + 1
			if dup > DUP_THRESHOLD:
				print '重复的条目已经达到上限，关闭爬虫'
				#os._exit(0)
		else:
			if len(replys) > 0:
				replys = replys[0]
			else:
				replys = ''
			tx.execute(\
				"insert into zbb (title, url, post_type, feed_type, content, replys, created) \
				values (%s, %s, %s, %s, %s, %s, %s)",\
				(title[0], item['url'], item['post_type'], item['feed_type'],
				 content[0], replys, created))

			tx.execute(\
				"insert into post (title, post_type, content, updated, created) \
				values (%s, %s, %s, %s, %s)",\
				(title[0], item['post_type'], '', created, created))
			post_id = tx.lastrowid
			print post_id

			tx.execute(\
				"insert into feed (feed_title, feed_type, post_type, post_id, updated, created) \
				values (%s, %s, %s, %s, %s, %s)",\
				(title[0], item['feed_type'], item['post_type'], post_id, created, created))

        	for url in box.xpath('./div[@class="content"]/a'):
        		video_title = url.xpath('strong/text()').extract()
        		if len(video_title) > 0:
        			video_link = url.xpath('@href').extract()[0]
        			if 'letv.com' in video_link:
        				video_vendor = 'letv'
        			if 'qq.com' in video_link:
        				video_vendor = 'qq'
        			if 'sina.com' in video_link:
        				video_vendor = 'sina'
        			if 'cntv.cn' in video_link:
        				video_vendor = 'cntv'
        			if 'youku.com' in video_link:
        				video_vendor = 'youku'
        			if 'pptv.com' in video_link:
        				video_vendor = 'pptv'

        			matchObj = re.match( r'(.*)] (.*)', video_title[0], re.M|re.I)
        			if matchObj:
        				video_name = matchObj.group(2)

        				tx.execute(\
							"insert into video (name, link, vendor, created) \
							values (%s, %s, %s, %s)",\
							(video_name, video_link, video_vendor, created))
        				video_id = tx.lastrowid

        				tx.execute(\
							"insert into object_video (video_id, obj_id, obj_type) \
							values (%s, %s, %s)",\
							(video_id, post_id, 'post'))

		#log.msg("Item stored in db: %s" % item, level=log.DEBUG)

	def handle_error(self, e):
		log.err(e)

	def check_contain_chinese(check_str):
		for ch in check_str.decode('utf-8'):
			if u'\u4e00' <= ch <= u'\u9fff':
				return True
			return False
