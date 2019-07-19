import scrapy
from weiboScrapy.items import WeiboItem, CommentItem
from scrapy.http import Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import time
import re
from lxml import etree
from weiboScrapy.spiders.utils import time_fix, extract_weibo_content, extract_comment_content
from urllib import parse


class TopicSpider(scrapy.Spider):
    name = "search_topic"
    base_url = "https://weibo.cn"

    # 构造查询页面的url
    key_words = ['#中国花协推荐牡丹为国花#', '#迪士尼乐园员工是假装开心#', '#韩商言借钱#',
                '#吴亦凡剪头发#', '#李现向反黑站道歉#', '#人均垃圾产量最多国家#', '#80名儿童贴三伏贴后被灼伤#',
                '#香港警方拘捕47人#', '#微信可同时设5个浮窗#', '#清华公布在京本科提档线#']
    start_urls = []
    for key_word in key_words:
        start_time = None
        end_time = 20190716
        base_search_url = 'https://weibo.cn/search/mblog?'
        page_num = 1
        url_data = {'hideSearchFrame': '',
                    'keyword': key_word,
                    'advancedfilter': 1,
                    'endtime': end_time,
                    'sort': 'hot',   # 'time' or 'hot'
                    'page': page_num}
        if start_time != None:
            url_data['starttime'] = start_time
        search_url = base_search_url + parse.urlencode(url_data)
        start_urls.append(search_url)

    # 起始爬取页面列表
    # start_urls = [search_url]

    def parse(self, response):
        # 搜索结果有多少页
        if response.url.endswith('page=1'):
            # 如果是第1页，一次性获取后面的所有页
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse, dont_filter=True, meta=response.meta)
        # 解析一个页面的结果
        tree_node = etree.HTML(response.body)
        weibo_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for weibo_node in weibo_nodes:
            try:
                weibo_item = WeiboItem()
                # weibo_item['crawl_time'] = int(time.time())
                weibo_item['crawl_time'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                weibo_item['nick_name'] = weibo_node.xpath('.//a[@class="nk"]/text()')[-1]

                # 如果是搜索结果的到的微博，保留搜索的query
                search_query = re.search(r'keyword=(.*?)&advancedfilter', response.url)
                if search_query:
                    weibo_item['search_query'] = parse.unquote(search_query.group(1))
                else:
                    weibo_item['search_query'] = 'None'

                    # 获取微博id 和 url
                weibo_repost_url = weibo_node.xpath('.//a[contains(text(),"转发[")]/@href')[0]
                user_weibo_id = re.search(r'/repost/(.*?)\?uid=(\d+)', weibo_repost_url)  # 微博和用户id在转发的url里有
                weibo_item['_id'] = '{}_{}'.format(user_weibo_id.group(2), user_weibo_id.group(1))
                weibo_item['user_id'] = user_weibo_id.group(2)
                weibo_item['weibo_url'] = 'https://weibo.com/{}/{}'.format(user_weibo_id.group(2),
                                                                           user_weibo_id.group(1))
                # 获取发表时间 发布设备
                create_time_info = weibo_node.xpath('.//span[@class="ct"]/text()')[0]
                if "来自" in create_time_info:
                    weibo_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
                    weibo_item['tool'] = create_time_info.split('来自')[1].strip()
                else:
                    weibo_item['created_at'] = time_fix(create_time_info.strip())
                    weibo_item['tool'] = 'None'

                # 转评赞信息
                like_num = weibo_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
                weibo_item['like_num'] = int(re.search('\d+', like_num).group())

                repost_num = weibo_node.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
                weibo_item['repost_num'] = int(re.search('\d+', repost_num).group())

                comment_num = weibo_node.xpath(
                    './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
                weibo_item['comment_num'] = int(re.search('\d+', comment_num).group())

                # 图片视频url，图片只有第一张图的url，要获取所有，需要去该url进行点击跳转
                images = weibo_node.xpath('.//img[@alt="图片"]/@src')
                if images:
                    weibo_item['image_url'] = images[0]
                else:
                    weibo_item['image_url'] = 'None'

                videos = weibo_node.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
                if videos:
                    weibo_item['video_url'] = videos[0]
                else:
                    weibo_item['video_url'] = 'None'

                # 位置信息，包括经纬度和位置
                map_node = weibo_node.xpath('.//a[contains(text(),"显示地图")]')
                if map_node:
                    map_node = map_node[0]
                    map_node_url = map_node.xpath('./@href')[0]
                    map_info = re.search(r'xy=(.*?)&', map_node_url).group(1)
                    weibo_item['location_map_info'] = map_info
                    weibo_item['location'] = map_node.xpath('./preceding-sibling::a/text()')[0]
                else:
                    weibo_item['location_map_info'] = 'None'
                    weibo_item['location'] = 'None'

                repost_node = weibo_node.xpath('.//a[contains(text(),"原文评论[")]/@href')
                if repost_node:  # 如果是转发微博
                    weibo_item['origin_weibo_url'] = repost_node[0]
                    # 如果是转发微博，则正文一定没有 阅读全文 直接获取正文内容
                    weibo_html = etree.tostring(weibo_node, encoding='unicode')
                    weibo_item['content'] = extract_weibo_content(weibo_html)
                    # 获取原微博正文内容
                    yield Request(repost_node[0], callback=self.parse_origin_weibo_content, meta={'item': weibo_item},
                                  priority=2)
                else:  # 是原创微博
                    weibo_item['origin_weibo_url'] = 'None'
                    weibo_item['origin_weibo_content'] = 'None'  # 无转发原博
                    all_content_link = weibo_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
                    if all_content_link:   # 如果有阅读全文
                        all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                        yield Request(all_content_url, callback=self.parse_all_content, meta={'item': weibo_item},
                                      priority=2)
                    else:
                        weibo_html = etree.tostring(weibo_node, encoding='unicode')
                        weibo_item['content'] = extract_weibo_content(weibo_html)
                        yield weibo_item

                # 爬取该条微博的评论信息
                comment_url = self.base_url + '/comment/' + weibo_item['weibo_url'].split('/')[-1] + '?page=1'
                yield Request(url=comment_url, callback=self.parse_comment, meta={'weibo_url': weibo_item['weibo_url']},
                              priority=1)

            except Exception as e:
                print(e)

    def parse_all_content(self, response):
        # 有阅读全文的情况，获取全文
        tree_node = etree.HTML(response.body)
        weibo_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        weibo_html = etree.tostring(content_node, encoding='unicode')
        weibo_item['content'] = extract_weibo_content(weibo_html)
        yield weibo_item

    def parse_origin_weibo_content(self, response):
        # 去原博页面爬取内容
        tree_node = etree.HTML(response.body)
        weibo_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        weibo_html = etree.tostring(content_node, encoding='unicode')
        weibo_item['origin_weibo_content'] = extract_weibo_content(weibo_html)
        yield weibo_item

    def parse_comment(self, response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_comment, dont_filter=True, meta=response.meta,
                                  priority=1)
        tree_node = etree.HTML(response.body)
        comment_nodes = tree_node.xpath('//div[@class="c" and contains(@id,"C_")]')
        for comment_node in comment_nodes:
            try:
                comment_user_url = comment_node.xpath('.//a[contains(@href,"/u/")]/@href')
                if not comment_user_url:
                    continue
                comment_item = CommentItem()
                # comment_item['crawl_time'] = int(time.time())
                comment_item['crawl_time'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                comment_item['weibo_url'] = response.meta['weibo_url']
                comment_item['comment_user_id'] = re.search(r'/u/(\d+)', comment_user_url[0]).group(1)
                comment_item['comment_user_nick_name'] = comment_node.xpath('.//a[contains(@href,"/u/")]/text()')[-1]
                comment_item['content'] = extract_comment_content(etree.tostring(comment_node, encoding='unicode'))
                comment_item['_id'] = comment_node.xpath('./@id')[0]
                created_at_info = comment_node.xpath('.//span[@class="ct"]/text()')[0]
                like_num = comment_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
                comment_item['like_num'] = int(re.search('\d+', like_num).group())
                comment_item['created_at'] = time_fix(created_at_info.split('\xa0')[0])
                yield comment_item

            except Exception as e:
                print(e)


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('search_topic')
    process.start()
