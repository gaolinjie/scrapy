#!/usr/bin/env python
# -*- coding:utf-8 -*-

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.http import Request, FormRequest
from scrapy import log
from meituan.items import MeituanItem

class MeituanSpider(CrawlSpider):
	name = "meituan"
	allowed_domains = ["www.meituan.com"]
	start_urls = [
		"http://www.meituan.com/lottery/past?mtt=1.index",
	]

	def parse(self, response) :
		sel = Selector(response)
		urls = sel.xpath('//ul[@class="deals-list cf"]/li/h4/a/@href').extract()
		for url in urls:
			print url
			yield Request(url, callback=self.parse_page) 

	def parse_page(self, response) :
		sel = Selector(response)

		item = MeituanItem()
		item['title']  = sel.xpath('//h1[@class="deal-component-title"]/text()').extract()
		item['intro']  = sel.xpath('//div[@class="deal-component-description"]/text()').extract()
		item['img']  = sel.xpath('//img[@class="focus-view"]/@src').extract()
		item['link']  = response.url
		item['created'] = sel.xpath('//div[@class="deal-term"]/dl/dd/text()').extract()

		return item