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

	def process_item(self, item, spider):
		query = self.dbpool.runInteraction(self._conditional_insert, item)
		query.addErrback(self.handle_error)
		return item

	def _conditional_insert(self, tx, item):
		if item['post_type'] == 'video':
			self.process_video(tx, item)
		if item['post_type'] == 'game':
			self.process_game(tx, item)


	def process_game(self, tx, item):
		post = self.get_post_info(item)

		tx.execute("select * from zbb where url=%s and post_type='game'", (item['url'],))
		result=tx.fetchone()

		if result:
			#更新post信息
			tx.execute("update post set title=%s where id=%s", (post['title'], result['post_id']))
			#url已处理过->对比视频数目
			if self.get_post_video_num(tx, item) != result['video_num']:
				#视频数量有变化需要更新
				if 'luxiang' in item['url']:
					self.update_luxiang_videos(tx, item, result['post_id'], post['created'], result['id'])
				else:
					self.update_post_videos(tx, item, result['post_id'], post['created'], result['id'])
		else:
			#url未处理过
			tx.execute(\
				"insert into zbb (title, url, post_type, feed_type, feed_uuid, content, replys, created) \
				values (%s, %s, %s, %s, %s, %s, %s, %s)",\
				(post['title'], item['url'], item['post_type'], item['feed_type'], item['feed_uuid'],
				 post['content'], post['replys'], post['created']))
			zbb_id = tx.lastrowid

			tx.execute(\
				"insert into post (title, post_type, feed_type, content, updated, created) \
				values (%s, %s, %s, %s, %s, %s)",\
				(post['title'], item['post_type'], item['feed_type'], '', post['created'], post['created']))
			post_id = tx.lastrowid

			tx.execute("select * from feed where feed_uuid=%s", (item['feed_uuid'],))
			result=tx.fetchone()

			if result:
				#url未处理过->有uuid对应feed->feed有一个post_id还没有值->更新url对应的post_id
				if 'jijin' in item['url']:
					tx.execute("update feed set post_id=%s where id=%s", (post_id, result['id']))
				if 'luxiang' in item['url']:
					tx.execute("update feed set post_id2=%s where id=%s", (post_id, result['id']))
			else:
				#url未处理过->没有uuid对应feed->直接insert new feed
				if 'jijin' in item['url']:
					tx.execute(\
						"insert into feed (feed_title, feed_type, post_type, feed_uuid, post_id, updated, created) \
						values (%s, %s, %s, %s, %s, %s, %s)",\
						(item['feed_title'], item['feed_type'], item['post_type'], item['feed_uuid'], post_id, post['created'], post['created']))
				if 'luxiang' in item['url']:
					tx.execute(\
						"insert into feed (feed_title, feed_type, post_type, feed_uuid, post_id2, updated, created) \
						values (%s, %s, %s, %s, %s, %s, %s)",\
						(item['feed_title'], item['feed_type'], item['post_type'], item['feed_uuid'], post_id, post['created'], post['created']))

			if 'luxiang' in item['url']:
				self.save_luxiang_videos(tx, item, post_id, post['created'], zbb_id)
			else:
				self.save_post_videos(tx, item, post_id, post['created'], zbb_id)

	def process_video(self, tx, item):
		post = self.get_post_info(item)

		tx.execute("select * from zbb where url=%s and post_type!='game'", (item['url'],))
		result=tx.fetchone()

		if result:
			#更新post信息
			tx.execute("update post set title=%s where id=%s", (post['title'], result['post_id']))
			#url已处理过->对比视频数目
			if self.get_post_video_num(tx, item) != result['video_num']:
				#视频数量有变化需要更新
				self.update_post_videos(tx, item, result['post_id'], post['created'], result['id'])
		else:
			#url未处理过
			tx.execute(\
				"insert into zbb (title, url, post_type, feed_type, feed_uuid, content, replys, created) \
				values (%s, %s, %s, %s, %s, %s, %s, %s)",\
				(post['title'], item['url'], item['post_type'], item['feed_type'], item['feed_uuid'], 
				 post['content'], post['replys'], post['created']))
			zbb_id = tx.lastrowid

			tx.execute(\
				"insert into post (title, post_type, feed_type, content, updated, created) \
				values (%s, %s, %s, %s, %s, %s)",\
				(post['title'], item['post_type'], item['feed_type'], '', post['created'], post['created']))
			post_id = tx.lastrowid

			tx.execute(\
				"insert into feed (feed_title, feed_type, post_type, post_id, updated, created) \
				values (%s, %s, %s, %s, %s, %s)",\
				(post['title'], item['feed_type'], item['post_type'], post_id, post['created'], post['created']))

			self.save_post_videos(tx, item, post_id, post['created'], zbb_id)

	def update_post_videos(self, tx, item, post_id, created, zbb_id):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		video_num = 0
		for url in box.xpath('./div[@class="content"]/a')[::-1]:
			video_title = url.xpath('strong/text()').extract()
			if len(video_title) <= 0:
				continue
			else:
				video_title = video_title[0]
				if video_title==' ' or video_title=='  ':
					video_title = url.xpath('strong/span/text()').extract()
					if len(video_title) <= 0:
						continue
					else:
						video_title = video_title[0]

			video_link = url.xpath('@href').extract()[0]
			video_vendor = self.get_video_vendor(video_link)

			matchObj = re.match( r'(.*)] (.*)', video_title, re.M|re.I)
			if not matchObj:
				continue

			video_name = matchObj.group(2)

			tx.execute("select * from video where name=%s and link=%s", (video_name, video_link))
			result=tx.fetchone()
			if result:
				#该视频以处理过
				video_id = result['id']
				tx.execute(\
					"select * from object_video where video_id=%s\
					and obj_id=%s and obj_type='post'", 
					(video_id, post_id))
				result2=tx.fetchone()
				if result2:
					#该post已经有该video
					pass
				else:
					tx.execute(\
						"insert into object_video (video_id, obj_id, obj_type) \
						values (%s, %s, %s)",\
						(video_id, post_id, 'post'))
					video_num = video_num + 1 
			else:
				#该视频未处理过，直接插入
				tx.execute(\
					"insert into video (name, link, vendor, created) \
					values (%s, %s, %s, %s)",\
					(video_name, video_link, video_vendor, created))
				video_id = tx.lastrowid

				tx.execute(\
					"insert into object_video (video_id, obj_id, obj_type) \
					values (%s, %s, %s)",\
					(video_id, post_id, 'post'))

				if item['post_type'] == 'game':
					section_name = self.get_section_name(item['feed_type'], video_title)

					tx.execute("select * from section where section_name=%s\
						and section_type=%s",\
						(section_name, item['feed_type']))
					result=tx.fetchone()

					if result:
						section_id = result['id']
						tx.execute(\
							"insert into section_video (video_id, section_id, post_id) \
							values (%s, %s, %s)",\
							(video_id, section_id, post_id))

				video_num = video_num + 1 

		tx.execute("update zbb set video_num=%s where id=%s", (video_num, zbb_id))


	def get_post_video_num(self, tx, item):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		video_num = 0
		for url in box.xpath('./div[@class="content"]/a'):
			video_title = url.xpath('strong/text()').extract()
			if len(video_title) <= 0:
				continue
			else:
				video_title = video_title[0]

			video_link = url.xpath('@href').extract()[0]
			video_vendor = self.get_video_vendor(video_link)

			matchObj = re.match( r'(.*)] (.*)', video_title, re.M|re.I)
			if not matchObj:
				continue

			video_num = video_num + 1
		return video_num

	def save_post_videos(self, tx, item, post_id, created, zbb_id):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		video_num = 0
		for url in box.xpath('./div[@class="content"]/a')[::-1]:
			video_title = url.xpath('strong/text()').extract()
			if len(video_title) <= 0:
				continue
			else:
				video_title = video_title[0]
				if video_title==' ' or video_title=='  ':
					video_title = url.xpath('strong/span/text()').extract()
					if len(video_title) <= 0:
						continue
					else:
						video_title = video_title[0]

			video_link = url.xpath('@href').extract()[0]
			video_vendor = self.get_video_vendor(video_link)

			matchObj = re.match( r'(.*)] (.*)', video_title, re.M|re.I)
			if not matchObj:
				continue

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

			if item['post_type'] == 'game':
				section_name = self.get_section_name(item['feed_type'], video_title)

				tx.execute("select * from section where section_name=%s\
					and section_type=%s",\
					(section_name, item['feed_type']))
				result=tx.fetchone()

				if result:
					section_id = result['id']
					tx.execute(\
						"insert into section_video (video_id, section_id, post_id) \
						values (%s, %s, %s)",\
						(video_id, section_id, post_id))

			video_num = video_num + 1    
		tx.execute("update zbb set video_num=%s, post_id=%s where id=%s", (video_num, post_id, zbb_id))    	

	def save_luxiang_videos(self, tx, item, post_id, created, zbb_id):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		video_num = 0
		for url in box.xpath('./div[@class="content"]/a')[::-1]:
			video_title = url.xpath('./text()').extract()
			if len(video_title) <= 0:
				video_title = url.xpath('strong/text()').extract()
				if len(video_title) <= 0:
					continue
				else:
					video_title = video_title[0]
			else:
				video_title = video_title[0]

			matchObj = re.match( r'(.*)] (.*)', video_title, re.M|re.I)
			if matchObj:
				video_name = matchObj.group(2)
			else:
				if video_title == '如有加时赛或者点球大战请点此进入观看':
					video_name = video_title
				else:
					continue

			video_link = url.xpath('@href').extract()[0]
			if video_link == 'http://www.zhibo8.cc/nba/luxiang.htm' or video_link == 'http://www.zhibo8.cc/zuqiu/luxiang.htm':
				continue
			video_vendor = self.get_video_vendor(video_link)

			tx.execute(\
				"insert into video (name, link, vendor, created) \
				values (%s, %s, %s, %s)",\
				(video_name, video_link, video_vendor, created))
			video_id = tx.lastrowid

			tx.execute(\
				"insert into object_video (video_id, obj_id, obj_type) \
				values (%s, %s, %s)",\
				(video_id, post_id, 'post'))

			if item['post_type'] == 'game':
				section_name = self.get_section_vendor(video_link)

				tx.execute("select * from section where section_name=%s\
					and section_type=%s",\
					(section_name, item['feed_type']))
				result=tx.fetchone()

				if result:
					section_id = result['id']
					tx.execute(\
						"insert into section_video (video_id, section_id, post_id) \
						values (%s, %s, %s)",\
						(video_id, section_id, post_id))

			video_num = video_num + 1    
		tx.execute("update zbb set video_num=%s, post_id=%s where id=%s", (video_num, post_id, zbb_id))   

	def update_luxiang_videos(self, tx, item, post_id, created, zbb_id):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		video_num = 0
		for url in box.xpath('./div[@class="content"]/a')[::-1]:
			video_title = url.xpath('./text()').extract()
			if len(video_title) <= 0:
				video_title = url.xpath('strong/text()').extract()
				if len(video_title) <= 0:
					continue
				else:
					video_title = video_title[0]
			else:
				video_title = video_title[0]

			matchObj = re.match( r'(.*)] (.*)', video_title, re.M|re.I)
			if matchObj:
				video_name = matchObj.group(2)
			else:
				if video_title == '如有加时赛或者点球大战请点此进入观看':
					video_name = video_title
				else:
					continue

			video_link = url.xpath('@href').extract()[0]
			if video_link == 'http://www.zhibo8.cc/nba/luxiang.htm' or video_link == 'http://www.zhibo8.cc/zuqiu/luxiang.htm':
				continue
			video_vendor = self.get_video_vendor(video_link)

			tx.execute("select * from video where name=%s and link=%s", (video_name, video_link))
			result=tx.fetchone()
			if result:
				#该视频以处理过
				video_id = result['id']
				tx.execute(\
					"select * from object_video where video_id=%s\
					and obj_id=%s and obj_type='post'", 
					(video_id, post_id))
				result2=tx.fetchone()
				if result2:
					#该post已经有该video
					pass
				else:
					tx.execute(\
						"insert into object_video (video_id, obj_id, obj_type) \
						values (%s, %s, %s)",\
						(video_id, post_id, 'post'))
					video_num = video_num + 1 
			else:
				#该视频未处理过，直接插入
				tx.execute(\
					"insert into video (name, link, vendor, created) \
					values (%s, %s, %s, %s)",\
					(video_name, video_link, video_vendor, created))
				video_id = tx.lastrowid

				tx.execute(\
					"insert into object_video (video_id, obj_id, obj_type) \
					values (%s, %s, %s)",\
					(video_id, post_id, 'post'))

				if item['post_type'] == 'game':
					section_name = self.get_section_vendor(video_link)

					tx.execute("select * from section where section_name=%s\
						and section_type=%s",\
						(section_name, item['feed_type']))
					result=tx.fetchone()

					if result:
						section_id = result['id']
						tx.execute(\
							"insert into section_video (video_id, section_id, post_id) \
							values (%s, %s, %s)",\
							(video_id, section_id, post_id))

				video_num = video_num + 1 

		tx.execute("update zbb set video_num=%s where id=%s", (video_num, zbb_id)) 	

	def get_section_name(self, feed_type, video_title):
		if feed_type == 'football':
			if 'CCTV新闻集锦' in video_title or '全场集锦' in video_title:
				section_name = '全场集锦'
			elif '进球视频' in video_title:
				section_name = '进球视频'
			elif '集锦' in video_title and not '全场集锦' in video_title:
				section_name = '个人集锦'
			else:
				section_name = '精彩花絮'
		elif feed_type == 'basketball':
			if 'CCTV全场集锦' in video_title or '全场集锦' in video_title:
				section_name = '全场集锦'
			elif '集锦' in video_title and not '全场集锦' in video_title:
				section_name = '个人集锦'
			else:
				section_name = '精彩花絮'

		return section_name

	def get_post_info(self, item):
		sel = Selector(item['response'])
		box = sel.xpath('//div[@id="main"]/div[@class="box"]')

		# 解析 title
		post_type = item['post_type']
		#if post_type == 'video':
		title = box.xpath('./div[@class="title"]/h1/text()').extract()
		if len(title) > 0:
			title  = title[0]
		else:
			title = ''
		print title
		#if post_type == 'game':
		#	title = item['feed_title']

		# 解析 content
		content = box.xpath('./div[@class="content"]').extract()
		if len(content) > 0:
			content = content[0]
		else:
			content = ''

		# 解析 replys
		replys = box.xpath('./div[@class="hot_huifu"]').extract()
		if len(replys) > 0:
			replys = replys[0]
		else:
			replys = ''
		
		# 解析 created
		created = box.xpath('./div[@class="title"]/span/text()').extract()
		if len(created) > 0:
			created = created[0]
			created = created.split('  ')
			if len(created) > 0:
				created = created[0]
			else:
				created = ''
		else:
			created = ''

		post = {}
		post['title'] = title
		post['content'] = content
		post['replys'] = replys
		post['created'] = created

		return post

	def get_video_vendor(self, video_link):
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
		if '56.com' in video_link:
			video_vendor = '56'
		if 'sinajs.cn' in video_link:
			video_vendor = 'miaopai'

		return video_vendor

	def get_section_vendor(self, video_link):
		if 'letv.com' in video_link:
			video_vendor = '乐视视频'
		if 'qq.com' in video_link:
			video_vendor = 'QQ视频'
		if 'sina.com' in video_link:
			video_vendor = '新浪视频'
		if 'cntv.cn' in video_link:
			video_vendor = 'CNTV视频'
		if 'youku.com' in video_link:
			video_vendor = '优酷视频'
		if 'pptv.com' in video_link:
			video_vendor = 'pptv'
		if '56.com' in video_link:
			video_vendor = '56'
		if 'sinajs.cn' in video_link:
			video_vendor = 'miaopai'

		return video_vendor

	def handle_error(self, e):
		log.err(e)

	def check_contain_chinese(check_str):
		for ch in check_str.decode('utf-8'):
			if u'\u4e00' <= ch <= u'\u9fff':
				return True
			return False
