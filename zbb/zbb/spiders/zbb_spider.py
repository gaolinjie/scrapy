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



class ZbbSipder(CrawlSpider) :
    name = "zbb"
    allowed_domains = ["www.zhibo8.cc"]
    start_urls = [
        "http://www.zhibo8.cc/zuqiu/index_old.htm"
    ]

    def parse(self, response) :
    	sel = Selector(response)

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
                    feed_uuid = uuid.uuid1()
                    sss = s.split(' <a href="')
                    title = sss[0]
                    url_jijin = sss[1]
                    url_jijin = 'http://www.zhibo8.cc' +  url_jijin[0:url_jijin.index('"')] 
                    if 'zuqiu' in url_jijin:
                        feed_type = 'football'
                    if 'jijin' in url_jijin:
                        yield Request(url_jijin, meta={'post_type': 'game', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': title}, 
                            callback=self.parse_page)
                    if len(sss) > 2:
                        url_luxiang = sss[2]
                        url_luxiang = 'http://www.zhibo8.cc' +  url_luxiang[0:url_luxiang.index('"')] 
                        if 'luxiang' in url_luxiang:
                            yield Request(url_luxiang, meta={'post_type': 'game', 'feed_type': feed_type, 'feed_uuid': feed_uuid, 'feed_title': title}, 
                                callback=self.parse_page)

        boxs = sel.xpath('//div[@class="box"]')
        i = 0
        for box in boxs:
            if i > 1:
                break
            i = i + 1
            urls = box.xpath('./div[@class="content"]/span/a/@href').extract()
            for url in urls[::-1]: 
                if 'zuqiu' in url:
                    feed_type = 'football'
                url = 'http://www.zhibo8.cc' + url
                print url
                if 'jijin' not in url and 'luxiang' not in url:
                    yield Request(url, meta={'post_type': 'video', 'feed_type': feed_type, 'feed_uuid': '', 'feed_title': ''}, 
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