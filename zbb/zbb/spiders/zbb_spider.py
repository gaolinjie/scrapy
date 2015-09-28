#!/usr/bin/env python
# -*- coding:utf-8 -*-

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.http import Request, FormRequest
from zbb.items import ZbbItem
from scrapy import log
import re
import uuid
import MySQLdb
import MySQLdb.cursors



class ZbbSipder(CrawlSpider) :
    name = "zbb"
    allowed_domains = ["www.zhibo8.cc"]
    start_urls = [
        "http://www.zhibo8.cc/zuqiu/index_old.htm",
        "http://www.zhibo8.cc/nba/index_old.htm"
    ]

    def parse(self, response) :
    	sel = Selector(response)

        conn = MySQLdb.connect(
            user='duang',
            passwd='duang',
            db='duang',
            host='localhost',
            cursorclass = MySQLdb.cursors.DictCursor,
            charset="utf8",
            use_unicode=True
            )
        cursor = conn.cursor()

        spans = sel.xpath('//div[@class="box"]/div[@class="content"]/span').extract()
        i = 0
        for span in spans:
            if i > 0:
                break
            i = i + 1
            span = span.replace('<span>','')
            span = span.replace(' | </span>','')
            for s in span.split(' | '):
                if not s.startswith('<a'):
                    sss = s.split(' <a href="')
                    title = sss[0]

                    if len(sss) == 2:
                        url1 = sss[1]
                        url1 = 'http://www.zhibo8.cc' +  url1[0:url1.index('"')] 

                        if 'zuqiu' in url1:
                            feed_type = 'football'
                        if 'nba' in url1:
                            feed_type = 'basketball'

                        feed_uuid = "%s" % uuid.uuid1()

                        if 'jijin' in url1 or 'luxiang' in url1:
                            yield Request(url1, meta={'post_type': 'game', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': title}, 
                                callback=self.parse_page)
                    elif len(sss) > 2:
                        url1 = sss[1]
                        url1 = 'http://www.zhibo8.cc' +  url1[0:url1.index('"')] 

                        url2 = sss[2]
                        url2 = 'http://www.zhibo8.cc' +  url2[0:url2.index('"')] 

                        if 'zuqiu' in url1:
                            feed_type = 'football'
                        if 'nba' in url1:
                            feed_type = 'basketball'

                        cursor.execute("select * from zbb where url=%s and post_type='game'", (url1,))
                        result=cursor.fetchone()

                        cursor.execute("select * from zbb where url=%s and post_type='game'", (url2,))
                        result2=cursor.fetchone()

                        if result or result2:
                            if result:
                                feed_uuid = result['feed_uuid']
                            if result2:
                                feed_uuid = result2['feed_uuid']
                        else:
                            feed_uuid = "%s" % uuid.uuid1()

                        if 'jijin' in url1 or 'luxiang' in url1:
                            yield Request(url1, meta={'post_type': 'game', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': title}, 
                                callback=self.parse_page)

                        if 'jijin' in url2 or 'luxiang' in url2:
                            yield Request(url2, meta={'post_type': 'game', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': title}, 
                                callback=self.parse_page)

        boxs = sel.xpath('//div[@class="box"]')
        i = 0
        for box in boxs:
            if i > 0:
                break
            i = i + 1
            urls = box.xpath('./div[@class="content"]/span/a/@href').extract()
            for url in urls[::-1]: 
                if 'zuqiu' in url:
                    feed_type = 'football'
                if 'nba' in url:
                    feed_type = 'basketball'
                url = 'http://www.zhibo8.cc' + url

                feed_uuid = "%s" % uuid.uuid1()

                if 'jijin' not in url and 'luxiang' not in url:
                    yield Request(url, meta={'post_type': 'video', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': ''}, 
                        callback=self.parse_page) 

    def parse_page(self, response) :
    	sel = Selector(response)

        item = ZbbItem()
        
        item['url']  = response.url
        item['post_type'] = response.meta['post_type']
        item['feed_type'] = response.meta['feed_type']
        item['feed_title'] = response.meta['feed_title']
        item['feed_uuid'] = response.meta['feed_uuid']
        item['response']  = response

        return item