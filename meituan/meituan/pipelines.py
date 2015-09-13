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
bucket_name = "mmm-cdn2"

dup = 0
DUP_THRESHOLD = 3

q = Auth(access_key, secret_key)
bucket = BucketManager(q)
source = "美团"

class MeituanPipeline(object):
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
		tx.execute("select * from post where title=%s", (item['title'][0],))
		result=tx.fetchone()
		#log.msg(result, level=log.DEBUG)
		print result
		if result:
			#log.msg("Item already stored in db:%s" % item, level=log.DEBUG)
			log.msg("Item already stored in db", level=log.DEBUG)
			#dup = dup + 1
			#if dup > DUP_THRESHOLD:
			#	print '重复的条目已经达到上限，关闭爬虫'
			#	os._exit(0)
		else:
			file_name = "m_" + "%s" % uuid.uuid1() + ".jpg"
			ret, info = bucket.fetch(item['img'][0], bucket_name, file_name)
			file_name = "http://7xii5h.com1.z0.glb.clouddn.com/" + file_name
			created = item['created'][2]
			tx.execute(\
				"insert into post (title, post_type, thumb) values (%s, %s, %s)",\
				(item['title'][0], "l", file_name))
		#log.msg("Item stored in db: %s" % item, level=log.DEBUG)

	def handle_error(self, e):
		log.err(e)