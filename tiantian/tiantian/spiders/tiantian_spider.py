import scrapy
import re

from tiantian.items import TiantianItem

class TiantianSpider(scrapy.Spider):
	name = "tiantian"
	allowed_domains = ["www.tiantian.tv"]
	start_urls = [
		"http://www.tiantian.tv/"
	]

	def parse(self, response):
		for sel in response.xpath('//div[@class="listcontent"]/div[@class="datelist"]'):
			dateheader = sel.xpath('./div[@class="dateheader"]/text()').extract()[0]
			dateheader = dateheader.split(' ')
			dateheader = dateheader[0]
			dateheader2 = dateheader[5:11]
			dateheader = re.sub(ur'[\u4e00-\u9fa5]', "-", dateheader)
			dateheader = dateheader[0:10]

			for ul in sel.xpath('./ul'):
				item = TiantianItem()

				time = ul.xpath('./li[@class="t1"]/text()').extract()[0]
				date = dateheader + ' ' + time + ':00'
				item['date'] = date
				item['sport'] = ul.xpath('./li[@class="t2"]/a/text()').extract()[0]
				item['game'] = ul.xpath('./li[@class="t3"]/a/text()').extract()[0]
				team = ul.xpath('./li[@class="t4"]/a/text()').extract()
				hot = 0
				if len(team) <= 0:
					team = ul.xpath('./li[@class="t4"]/a/strong/text()').extract()[0]
					style =  ul.xpath('./li[@class="t4"]/a/@style').extract()[0]
					if style == 'color:#000':
						hot = 1
					elif style == 'color:#f00':
						hot = 2
				else:
					team = team[0]
					hot = 0
				item['team'] = team
				item['hot'] = hot
				item['signal'] = ul.xpath('./li[@class="t5"]/a').extract()
				item['title'] = dateheader2 + ' ' + time +  ' ' + item['team']

				yield item