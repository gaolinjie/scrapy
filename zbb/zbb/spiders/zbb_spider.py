#!/usr/bin/env python
# -*- coding:utf-8 -*-

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.http import Request, FormRequest
from zbb.items import ZbbItem
from scrapy import log
import re



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
            if i > 1:
                break
            i = i + 1
            span = span.replace('<span>','')
            span = span.replace(' | </span>','')
            print '-----------------------------------------------------------------------'
            for s in span.split('|'):
                if not s.startswith(' <a'):
                    print s
        return

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
                    yield Request(url, meta={'post_type': 'video', 'feed_type': feed_type}, 
                        callback=self.parse_page) 

    def parse_page(self, response) :
    	sel = Selector(response)

        item = ZbbItem()
        
        item['url']  = response.url
        item['post_type'] = response.meta['post_type']
        item['feed_type'] = response.meta['feed_type']
        item['response']  = response

        return item