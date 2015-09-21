# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZbbItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    post_type = scrapy.Field()
    feed_type = scrapy.Field()
    feed_title = scrapy.Field()
    feed_uuid = scrapy.Field()
    response = scrapy.Field()
