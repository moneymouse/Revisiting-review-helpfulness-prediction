from __future__ import annotations
import json

import os
import sys
import csv
import random
import time
import pandas as pd

import requests
from lxml import etree

sys.path.append(os.getcwd())  # 修复嵌入式 Python 的导入问题

# from proxy import LocalProxyPool, Proxy, YouChengProxyPool

MIN_IMAGES_NUM = 1  # 最少图片数量，如果少于这个数量则跳过这条评论
MIN_TEXT_LENGTH = 20  # 最短评论长度
# FIRST = 10130  # 用于断点续传
FIRST = 1
#URL = "https://www.yelp.com//biz/bacchanal-buffet-las-vegas-9"  # 需要爬取的餐馆 url
TIMEOUT = 15  # 请求超时
PROXY_URL = "https://h.shanchendaili.com/api.html?action=get_ip&key=HUf8af2f1c09212152278CIw&time=10&count=1&protocol=http&type=json&only=0"

HEADERS_LIST = [
    {
        "Referer": "https://www.yelp.com/",
        "Origin": "https://www.yelp.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44",
        "Host": "www.yelp.com",
        "sec-fetch-size": "cross-site",
        "sec-ch-ua": '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        "DNT": "1",
    },
    {
        "Referer": "https://www.yelp.com/",
        "Origin": "https://www.yelp.com",
        "User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; "
                      "Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; "
                      "Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
        "Host": "www.yelp.com",
        "sec-fetch-size": "cross-site",
    },
    {
        "Referer": "https://www.yelp.com/",
        "Origin": "https://www.yelp.com",
        "User-Agent": "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
        "Host": "www.yelp.com",
        "sec-fetch-size": "cross-site",
        "sec-ch-ua": '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        "DNT": "1",
    },
    {
        "Referer": "https://www.yelp.com/",
        "Origin": "https://www.yelp.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/8.0",
        "Host": "www.yelp.com",
        "sec-fetch-size": "cross-site",
        "sec-ch-ua": '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        "DNT": "1",
    },
]  # 请求头
COLUMN_NAMES = [
    "rest name",
    "address",
    "price",
    "star",
    "reviews count",
    "class",
    "author",
    "from",
    "elite",
    "author friends",
    "author reviews",
    "author photos",
    "join date",
    "author useful votes",
    "author funny votes",
    "author cool votes",
    "rating_distribution_5",
    "rating_distribution_4",
    "rating_distribution_3",
    "rating_distribution_2",
    "rating_distribution_1",
    "review text",
    "review date",
    "review rating",
    "totalPhotos",
    "useful",
    "funny",
    "cool",
    "isUpdated",
    "businessOwnerReplies",
    "businessOwnerRepliesdate",
    "photo_1 link",
    "photo_2 link",
    "photo_3 link",
    "photo_4 link",
    "photo_5 link",
    "photo_6 link",
    "photo_7 link",
    "photo_8 link",
    "photo_9 link",
    "photo_10 link",
]  # 保存的数据项


# pool = LocalProxyPool()
session = requests.Session()  # 持久会话

def common_get(url) -> requests.Response:
    res = requests.get(url,headers=HEADERS_LIST[0])
    print(f"{url}:{res.ok}")
    time.sleep(0.005)
    return res

def write_row(filename: str, row: dict):
    """
    将 row 添加在文件 filename 中
    """
    # todo: 把w+改成a+
    with open(filename, newline='', encoding='utf-8-sig', mode='w+') as f:
        writer = csv.DictWriter(f, COLUMN_NAMES, extrasaction="ignore")  # 忽略不在 COLUMN_NAMES 中的项目
        if f.tell() == 0:  # 第一次保存，先打印列名
            writer.writeheader()
        writer.writerow(row)


def log(*text):
    print(f"[{time.strftime('%X')}]", *text, flush=True)


def main():
    df1 = pd.read_excel('a.xlsx')
    list = df1.values.tolist()
    list = [b for i in list for b in i]
    for hf_url in list:

        url = "https://www.yelp.com/" + hf_url
        resp = common_get(url)  # 使用代理请求网页
        if resp is None:
            raise Exception(f"无代理可请求网页（{url}），或所有代理已达最大使用次数，稍后再试")

        parser = etree.HTML(resp.text)  # 解析网页
        meta = parser.xpath('//meta[@name="yelp-biz-id"]/@content')[0]  # 该酒店的唯一ID，用于构建 comment_url
        page_data = {
            "address": "\n".join(parser.xpath('//address//span/text()')),
            "dist": "".join(parser.xpath('//address/following-sibling::p/text()')),
            "name": "".join(parser.xpath("//h1/text()")),
            "price": parser.xpath('//div[@data-testid="photoHeader"]//span[2]//text()')[0],
            "star": "".join(parser.xpath('//div[2]/div[1]/span/div/@aria-label')).split()[0],
            "classall": "-".join(parser.xpath('//div[@data-testid="photoHeader"]//span[3]/span/a/text()')),
        }  # 提取该页面数据
        filename = f"./data/{page_data['name']}.csv"  # 保存数据的文件以 csv 格式存储

        # 爬取评论
        # 评论 url 模板
        template = f"https://www.yelp.com/biz/{meta}/review_feed?rl=en&q=&sort_by=relevance_desc&start={{start}}"
        start = total_reviews = FIRST  # 从 FIRST 开始

        while start < total_reviews or start == FIRST:
            comment_url = template.format(start=start)  # 评论数据的 url
            # 使用代理请求数据
            resp = common_get(comment_url)
            if resp is None and start != FIRST:  # 无可用代理，保存当前数据并退出
                raise Exception(f"无代理可访问 {comment_url}，或所有代理已达最大使用次数，稍等后修改 FIRST = {start} 再重新运行")

            data = resp.json()
            reviews = data["reviews"]
            with open("comment_example.json","w+") as f:
                print(f"Save {comment_url} reviews into comment_example.json")
                json.dump(reviews,f)

            if data and start == FIRST:  # 第一次遍历需要更新评论数量
                total_reviews = data["pagination"]["totalResults"]
                log("需要爬取", url, "共", total_reviews - FIRST, "条评论")

            for review in reviews:  # 提取数据

                # 预处理
                # 跳过useful过低评论
                if(review["feedback"]["counts"]["useful"]<1):
                    print(review["id"])
                    continue

                # 往 row 中添加评论用户信息
                user_url = f"https://www.yelp.com{review['user']['link']}"
                
                resp = common_get(user_url)
                # 尝试绕过防火墙
                # if random.choice([True, *([False] * 3)]):
                #     proxy_get(url)
                if resp is None:
                    raise Exception(f"无可用代理访问 {comment_url}，餐厅链接{url},或所有代理已达最大使用次数，稍等后修改 FIRST = {start} 再重新运行")

                parser = etree.HTML(resp.text)

                # 提取投票信息
                votes = []
                votes_node = parser.xpath('//ul[@class="ylist ylist--condensed"]')
                if votes_node:
                    votes_node = parser.xpath('//ul[@class="ylist ylist--condensed"]')[0]
                    votes = [name.strip().lower() for name in votes_node.xpath('.//text()') if name.strip()]
                votes = {
                    "userful": votes[votes.index("userful") + 1] if "userful" in votes else "0",
                    "funny": votes[votes.index("funny") + 1] if "funny" in votes else "0",
                    "cool": votes[votes.index("cool") + 1] if "cool" in votes else "0",
                }

                # 用户评分分布表格
                rating_distribution = (parser.xpath('//table[contains(@class, "histogram")][1]'
                                                    '//td[contains(@class, "histogram_count")]/text()')
                                        or ["0"] * 5)

                # 用户注册时间
                since = (
                    parser.xpath('//div[@class="user-details-overview_sidebar"]/div[5]/ul/li[2]/p/text()')[0]
                    if parser.xpath('//div[@class="user-details-overview_sidebar"]/div[5]/ul/li[2]/p/text()')
                    else None
                )

                try:
                    row = {
                        "rest name": review["business"]["name"],  # 酒店名称
                        "address": page_data['address'] + '\n' + page_data['dist'],
                        "price": page_data['price'],
                        'star': page_data['star'],
                        'reviews count': total_reviews,
                        'class': page_data['classall'],
                        'author': review["user"]["markupDisplayName"],
                        'from': review["user"]["displayLocation"],
                        'elite': review["user"]["eliteYear"],
                        'author friends': review["user"]["friendCount"],
                        'author reviews': review["user"]["reviewCount"],
                        'author photos': review["user"]["photoCount"],
                        'join date': since,
                        'author useful votes': votes["userful"],
                        'author funny votes': votes["funny"],
                        'author cool votes': votes["cool"],
                        'rating_distribution_5': rating_distribution[0],
                        'rating_distribution_4': rating_distribution[1],
                        'rating_distribution_3': rating_distribution[2],
                        'rating_distribution_2': rating_distribution[3],
                        'rating_distribution_1': rating_distribution[4],
                        'review text': review["comment"]["text"],
                        'review date': review["localizedDate"],
                        'review rating': review["rating"],
                        'totalPhotos': review["totalPhotos"],
                        'useful': review["feedback"]["counts"]["useful"],
                        'funny': review["feedback"]["counts"]["funny"],
                        'cool': review["feedback"]["counts"]["cool"],
                        'isUpdated': review["isUpdated"],
                        'businessOwnerReplies': True if review["businessOwnerReplies"] else False,
                        'businessOwnerRepliesdate': (review["businessOwnerReplies"][0]["localizedDate"]
                                                        if review["businessOwnerReplies"] else None),
                        **{  # 图片链接
                            f"photo_{i} link": photo["url"]
                            for i, photo in enumerate(review["lightboxMediaItems"], start=1)
                        }
                    }
                except (KeyError, IndexError) as e:
                    raise Exception(f"出现异常，已爬取数据保存在 {filename} 中，"
                                    f"修复后修改 FIRST = {start} 再重新运行") from e

                write_row(filename, row)  # 将当前评论数据写入文件中
                # for end

            start += 10  # 每次爬取 10 条评论
            # while end

    # 完成
    with open("done.txt", mode="a", encoding='utf8') as f:
            f.write(f"{filename}\t{url}\n")
    log("已爬取", url, "共计", total_reviews - FIRST, "条评论")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        print(flush=True)
        os.system("pause")