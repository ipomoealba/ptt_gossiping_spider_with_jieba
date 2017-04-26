# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class PostItem(scrapy.Item):
    title = scrapy.Field()
    date = scrapy.Field()
    author = scrapy.Field()
    author_ip = scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()
    sheeee = scrapy.Field()
    push = scrapy.Field()
    arrow = scrapy.Field()
    comments = scrapy.Field()
    content_keywords = scrapy.Field()
    comment_keywords = scrapy.Field()
    first_page = scrapy.Field()
    relink = scrapy.Field()
