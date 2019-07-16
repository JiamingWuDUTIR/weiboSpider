# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from weiboScrapy.items import WeiboItem, CommentItem
import pymongo
from pymongo.errors import DuplicateKeyError


class WeiboscrapyPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client['weiboDemo']
        self.Weibos = db["Weibos"]
        self.Comments = db["Comments"]

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            # self.insert_item(self.Weibos, item)
            print(item['nick_name'])
            print(item['content'])
            print('*' * 100)
        elif isinstance(item, CommentItem):
            # self.insert_item(self.Comments, item)
            print(item['comment_user_nick_name'])
            print(item['content'])
            print('*' * 100)

    @staticmethod
    def insert_item(collection, item):
        try:
            collection.update({"_id": item['_id']}, dict(item), upsert=True)
            # collection.insert(dict(item))
        except DuplicateKeyError:
            """
            说明有重复数据
            """
            pass
