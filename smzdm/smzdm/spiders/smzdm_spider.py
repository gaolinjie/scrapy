#!/usr/bin/env python
# -*- coding:utf-8 -*-

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.http import Request, FormRequest
from smzdm.items import SmzdmItem
from scrapy import log
import re



class SmzdmSipder(CrawlSpider) :
    name = "smzdm"
    allowed_domains = ["www.smzdm.com",
    					"haitao.smzdm.com",
    					"news.smzdm.com"]
    start_urls = [
        "http://www.smzdm.com/p1"
    ]

    def parse(self, response) :
    	sel = Selector(response)
    	urls = sel.xpath('//div[@class="list "]/div[@class="listTitle"]/h4/a/@href').extract()  
        for url in urls:  
            print url
            yield Request(url, callback=self.parse_page) 

        url_pattern = re.compile(r'http://www.smzdm.com/p(\d+)') 
        url_match = url_pattern.search(response.url) 
        if url_match: 
        	page_num = int(url_match.group(1)) + 1
        	print "dfdsfdsafdsafasfdsfsadf" + str(page_num)
        	if page_num < 4:
        		next_url = "http://www.smzdm.com/p" + str(page_num)
        		yield Request(next_url, callback=self.parse)
        	else:
        		raise CloseSpider('已经爬取到第10页')
        else:
        	print '获取下一页url失败'
        	raise CloseSpider('获取下一页url失败')

    def parse_page(self, response) :
    	sel = Selector(response)
        topic = sel.xpath('//article')

        item = SmzdmItem()
        item['title']  = topic.xpath('./h1/text()').extract()
        item['subtitle']  = topic.xpath('./h1/span[@class="red"]/text()').extract()
        item['intro']  = topic.xpath('./div[@class="news_content"]/p/text()').extract()
        item['content']  = topic.xpath('./div[@class="news_content"]/p').extract()
        item['img']  = topic.xpath('./div[@class="news_content"]/div[@class="article_picwrap"]/a[@class="picLeft"]/img/@src').extract()
        item['link']  = response.url
        item['dlink']  = topic.xpath('./div[@class="news_content"]/div[@class="article_picwrap"]/div[@class="buy"]/a/@href').extract()
        item['tag']  = topic.xpath('./div[@class="article_meta"]/span[@class="lFloat"]/text()').extract()
        item['vendor']  = topic.xpath('./div[@class="news_content"]/div[@class="article_picwrap"]/a[@class="mall"]/text()').extract()
        item['author_name'] = topic.xpath('./div[@class="article_meta"]/div[@class="recommend"]/text()').extract()
        item['up_num'] = sel.xpath('//div[@class="score_rateBox"]/span[@class="red"]/text()').extract()
        item['down_num'] = sel.xpath('//div[@class="score_rateBox"]/span[@class="grey"]/text()').extract()
        item['reply_num'] = sel.xpath('//div[@class="leftLayer"]/em[@class="commentNum"]/text()').extract()
        item['follow_num'] = sel.xpath('//div[@class="leftLayer"]/a[@class="fav"]/em/text()').extract()
        item['created'] = topic.xpath('./div[@class="article_meta"]/span[@class="lrTime"]/text()').extract()

        return item