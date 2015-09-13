#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import log
from twisted.enterprise import adbapi
from scrapy.http import Request

import MySQLdb
import MySQLdb.cursors
import os
import uuid

from qiniu import Auth
from qiniu import BucketManager

access_key = "DaQzr1UhFQD6im_kJJjZ8tQUKQW7ykiHo4ZWfC25"
secret_key = "Ge61JJtUSC5myXVrntdVOqAZ5L7WpXR_Taa9C8vb"
bucket_name = "mmm-cdn"

dup = 0
DUP_THRESHOLD = 3

q = Auth(access_key, secret_key)
bucket = BucketManager(q)
source = "什么值得买"

class SmzdmPipeline(object):
	def __init__(self):
		self.dbpool = adbapi.ConnectionPool('MySQLdb',
			db = 'avati',
			user = 'avati',
			passwd = 'avati',
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
		tx.execute("select * from sitem where title=%s", (item['title'][0],))
		result=tx.fetchone()
		#log.msg(result, level=log.DEBUG)
		print result
		if result:
			#log.msg("Item already stored in db:%s" % item, level=log.DEBUG)
			log.msg("Item already stored in db", level=log.DEBUG)
			dup = dup + 1
			if dup > DUP_THRESHOLD:
				print '重复的条目已经达到上限，关闭爬虫'
				os._exit(0)
		else:
			file_name = "%s" % uuid.uuid1() + ".jpg"
			ret, info = bucket.fetch(item['img'][0], bucket_name, file_name)
			file_name = "http://7xii5h.com1.z0.glb.clouddn.com/" + file_name
			created = item['created'][0] + ":00"
			tx.execute(\
				"insert into post (title, subtitle, content, post_type, thumb, link, dlink, source, price, vendor, author_name, up_num, down_num, reply_num, follow_num, created) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
				(item['title'][0], item['subtitle'][0], item['content'][0], "l", file_name, item['link'], item['dlink'][0], source, item['price'][0], item['vendor'][0], item['author_name'], item['up_num'][0], item['down_num'][0], item['reply_num'][0], item['follow_num'][0], created))
		#log.msg("Item stored in db: %s" % item, level=log.DEBUG)

	def handle_error(self, e):
		log.err(e)