# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TiantianItem(scrapy.Item):
    date = scrapy.Field()
    sport = scrapy.Field()
    game = scrapy.Field()
    team = scrapy.Field()
    signal = scrapy.Field()
    hot = scrapy.Field()
    title = scrapy.Field()
