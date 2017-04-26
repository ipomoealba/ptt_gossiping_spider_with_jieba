#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: pipeline.py
import MySQLdb
import MySQLdb.cursors
import json

from twisted.enterprise import adbapi
from hashlib import md5
from scrapy import log


class MySQLStorePipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        d = self.dbpool.runInteraction(self._do_upinsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d

    def _do_upinsert(self, conn, item, spider):
        urlmd5id = self._get_urlmd5id(item)
        try:
            sql = """
            SELECT * FROM `Gossiping` WHERE title like '%s';
            """ % ('%' + item["title"] + '%')
            result = conn.execute(sql)
            item["first_page"] = 1
            item["relink"] = 0
            if result != 0:
                print("this link is not first_page.")
                sql = """
                SELECT * FROM `Gossiping` WHERE `ptime` < '%s' and title like '%s';
                """ % (item["date"], '%' + item["title"] + '%')
                print(sql)
                result2 = conn.execute(sql)
                if result2 == 0:
                    item["first_page"] = 1
                    sql = """
                    UPDATE `Gossiping` SET first_page = 0, relink = '%d' + 1 WHERE `ptime` < '%s' and title like '%s';
                    """ % (item["date"], result, '%' + item["title"] + '%')
                    conn.execute(sql)
                else:
                    item["first_page"] = 0
                item["relink"] = result
            else:
                print("this is first page")
            x = json.dumps(item["comments"]).decode('unicode-escape')
            content_keyword = json.dumps(
                item['content_keywords']).decode('unicode-escape')
            comment_keyword = json.dumps(
                item['comment_keywords']).decode('unicode-escape')
            sql = """
            insert into Content(pid,content)
            values('%s','%s')""" % (urlmd5id, r''.join(item["content"]))
            conn.execute(sql)
            # print("first")
            sql = """insert into Reply(pid,reply)
            values('%s', '%s');
            """ % (urlmd5id, r''.join(x))
            conn.execute(sql)
            # print("second")
            sql = """INSERT INTO Gossiping(pid, title, ptime, arthor, ip
            , content_keywords, comment_keywords, push, sheee, arrow,
            first_page, relink )
            Values('%s', '%s', '%s', '%s', '%s', '%s',
            '%s','%d', '%d', '%d', '%d', '%d');
            """ % (urlmd5id, item["title"], item["date"], item["author"],
                   item["author_ip"], r''.join(content_keyword),
                   r''.join(comment_keyword), item['push'], item['sheeee'],
                   item['arrow'], item["first_page"], item["relink"])
            conn.execute(sql)
            # print("third")
        except Exception as e:
            print(str(e))
            print("this data has in the db")

    def _get_urlmd5id(self, item):
        return md5(item['url']).hexdigest()

    def _handle_error(self, failure, item, spider):
        log.err(failure)
