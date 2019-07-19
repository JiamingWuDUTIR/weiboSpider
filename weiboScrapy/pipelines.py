# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from weiboScrapy.items import WeiboItem, CommentItem
import pymongo
from pymongo.errors import DuplicateKeyError
import mysql.connector


class MongoPipeline(object):
    """
    MongoDB Pipeline
    记得修改setting，确定使用哪个pipeline class
    """
    def __init__(self):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client['weiboDemo']
        self.Weibos = db["Weibos"]
        self.Comments = db["Comments"]

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            self.insert_item(self.Weibos, item)
            # print(item['nick_name'])
            # print(item['content'])
            # print('*' * 100)
        elif isinstance(item, CommentItem):
            self.insert_item(self.Comments, item)
            # print(item['comment_user_nick_name'])
            # print(item['content'])
            # print('*' * 100)

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


class MySQLPipeline(object):
    """
    MySQL Pipeline
    记得修改setting，确定使用哪个pipeline class
    """
    def __init__(self):
        self.coon = mysql.connector.connect(user='root', password='password',
                                            host="localhost", port='3306')
        self.cursor = self.coon.cursor(buffered=True)
        self.cursor.execute('use weibodemo;')

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            try:
                # 先删掉这条数据
                str_del = 'delete from weibo_table where W_weibo_id = "{}";'.format(item['_id'])
                self.cursor.execute(str_del)
                # 将正文中的单双引号转义
                content = item['content'].replace('"', r'\"')
                content = content.replace("'", r"\'")
                str_insert = 'insert into weibo_table (W_weibo_id, W_search_query, W_nick_name, W_weibo_url, \
                W_created_at, W_like_num, W_repost_num, W_comment_num, W_content, W_user_id, W_image_url, W_video_url, \
                W_tool, W_location, W_location_map_info, W_origin_weibo_url, W_origin_weibo_content, W_crawl_time) '
                str_value = 'values ("{}", "{}", "{}", "{}", "{}",  \
                            {}, {}, {}, "{}", "{}",  \
                            "{}", "{}", "{}", "{}", "{}",  \
                            "{}", "{}", "{}"); '.\
                            format(item['_id'], item['search_query'], item['nick_name'], item['weibo_url'], item['created_at'],
                                  item['like_num'], item['repost_num'], item['comment_num'], content, item['user_id'],
                                  item['image_url'], 'None', item['tool'], item['location'], item['location_map_info'],
                                  item['origin_weibo_url'], item['origin_weibo_content'], item['crawl_time'])
                str_insert_all = str_insert + str_value
                self.cursor.execute(str_insert_all)
                self.coon.commit()
                # print(item['nick_name'])
                # print(item['content'])
                # print('*' * 100)
            except:
                print('error!')
                
        elif isinstance(item, CommentItem):
            try:
                str_del = 'delete from comment_table where C_comment_id = "{}";'.format(item['_id'])
                self.cursor.execute(str_del)

                content = item['content'].replace('"', r'\"')
                content = content.replace("'", r"\'")
                str_insert = 'insert into comment_table (C_comment_id, C_comment_user_id, C_comment_user_nick_name, \
                            C_content, C_weibo_url, C_like_num, C_created_at, C_crawl_time) '
                str_value = 'values ("{}", "{}", "{}", "{}", "{}", {}, "{}", "{}"); '. \
                    format(item['_id'], item['comment_user_id'], item['comment_user_nick_name'], content,
                           item['weibo_url'], item['like_num'], item['created_at'], item['crawl_time'])
                str_insert_all = str_insert + str_value
                self.cursor.execute(str_insert_all)
                self.coon.commit()
                # print(item['nick_name'])
                # print(item['content'])
                # print('*' * 100)
            except:
                print('error!')
