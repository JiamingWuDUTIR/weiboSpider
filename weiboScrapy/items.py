# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class WeiboItem(Item):
    """ 微博信息 """
    _id = Field()  # 微博id
    search_query = Field()  # 如果是通过搜索得到的，则为搜索query
    nick_name = Field()  # 微博昵称
    weibo_url = Field()  # 微博URL
    created_at = Field()  # 微博发表时间
    like_num = Field()  # 点赞数  int
    repost_num = Field()  # 转发数  int
    comment_num = Field()  # 评论数  int
    content = Field()  # 微博内容
    user_id = Field()  # 发表该微博用户的id
    image_url = Field()  # 图片
    video_url = Field()  # 视频
    tool = Field()  # 发布微博的工具
    location = Field()  # 定位信息
    location_map_info = Field()  # 经纬度信息
    origin_weibo_url = Field()  # 原始微博url，只有转发的微博才有这个字段
    origin_weibo_content = Field()  # 原始微博，只有转发的微博才有这个字段
    crawl_time = Field()  # 抓取时间戳


class CommentItem(Item):
    """
    微博评论信息
    """
    _id = Field()
    comment_user_id = Field()  # 评论用户的id
    comment_user_nick_name = Field()  # 评论用户的昵称
    content = Field()  # 评论的内容
    weibo_url = Field()  # 评论的微博的url
    like_num = Field()  # 点赞数  int
    created_at = Field()  # 评论发表时间
    crawl_time = Field()  # 抓取时间戳

