# -*-coding:utf-8-*-
#从数据库获取url爬取酒店详细信息

import sys
import os
import datetime
import requests
from lxml import etree
import simplejson
import traceback
from public.city import City
from public.mysqlpooldao import MysqlDao
from public.headers import Headers

file_path = os.path.dirname(os.path.abspath(__file__))

reload(sys)
sys.setdefaultencoding('utf-8')

mysql_dao = MysqlDao()

def get_last_page(url):
    last_page = 1
    try:
        headers = Headers.get_headers()
        req = requests.get(url, headers=headers, timeout=5)
        if req.status_code == 200:
            req.encoding = 'utf8'
            html = req.text
            selector = etree.HTML(html)
            last_pages = selector.xpath('//div[@id="page_info"]/div[@class="c_page_list layoutfix"]/a[@rel="nofollow"]/text()')
            if len(last_pages) > 0:
                last_page = int(last_pages[0])
    except Exception as e:
        traceback.print_exc()
        print(e)
    return last_page

def get_hotel_url(url, last_page, city_name, city_id, district_name):
    page = last_page
    while True:
        if page <= 0:
            break
        target_url = url + 'p' + str(page)
        headers = Headers.get_headers()
        try:
            req = requests.get(target_url, headers=headers, timeout=5)
        except Exception as e:
            traceback.print_exc()
            print(e)
            continue
        if req.status_code == 200 or req.status_code == 403 or req.status_code == 404:
            req.encoding = 'utf8'
            html = req.text
            selector = etree.HTML(html)
            hotels = selector.xpath('//div[@id="hotel_list"]/div/ul')

            for hotel in hotels:
                # 酒店名
                hotel_names = hotel.xpath('li[@class="searchresult_info_name"]/h2/a/@title')
                if hotel_names:
                    hotel_name = hotel_names[0].replace(' ', '').replace('\r\n', '')
                else:
                    hotel_name = u'无'
                # 酒店网址
                hotel_add_urls = hotel.xpath('li[@class="searchresult_info_name"]/h2/a/@href')
                if hotel_add_urls:
                    hotel_add_url = hotel_add_urls[0]
                    hotel_url = 'http://hotels.ctrip.com' + hotel_add_url
                    hotel_id = hotel_add_url.replace('/hotel/', '').replace('.html?isFull=F', '').replace('.html?isFull=T', '')
                else:
                    hotel_url = u'无'
                    hotel_id = u'无'
                # 图片
                imgs = hotel.xpath('li[@class="pic_medal"]/div[@class="hotel_pic"]/a/img/@src')
                if imgs:
                    img = imgs[0].replace(' ', '').replace('\r\n', '')
                else:
                    img = u'无'
                # 星级
                stars = hotel.xpath('li[@class="searchresult_info_name"]/p[@class="medal_list"]/span/@title')
                if stars == ['', ] or stars == False:
                    star = u'无'
                else:
                    star = stars[0].replace(' ', '').replace('\r\n', '')
                # 地址
                addresses = hotel.xpath('li[@class="searchresult_info_name"]/p[@class="searchresult_htladdress"]/text()')
                if addresses:
                    address = ''.join(addresses).replace(' ', '').replace('\r\n', '').replace(u'【', '').replace(u'】',                                                                                                '')
                else:
                    address = u'无'
                # 商圈
                regions = hotel.xpath('li[@class="searchresult_info_name"]/p[@class="searchresult_htladdress"]/a/text()')
                if regions:
                    region = regions[0].replace(' ', '').replace('\r\n', '')
                else:
                    region = u'无'
                # 评分
                scores = hotel.xpath(
                    'li[@class="searchresult_info_judge "]/div[@class="searchresult_judge_box"]/a[@class="hotel_judge"]/span[@class="hotel_value"]/text()')
                if scores:
                    score = scores[0].replace(' ', '').replace('\r\n', '')
                else:
                    score = u'无'
                # 用户推荐
                recommends = hotel.xpath(
                    'li[@class="searchresult_info_judge "]/div[@class="searchresult_judge_box"]/a[@class="hotel_judge"]/span[@class="total_judgement_score"]/text()')
                if recommends:
                    recommend = recommends[-1].replace(' ', '').replace('\r\n', '')
                else:
                    recommend = u'无'
                # 点评
                comment_nums = hotel.xpath(
                    'li[@class="searchresult_info_judge "]/div[@class="searchresult_judge_box"]/a[@class="hotel_judge"]/span[@class="hotel_judgement"]/text()')
                if comment_nums:
                    comment_num = comment_nums[0].replace(' ', '').replace('\r\n', '')
                else:
                    comment_num = u'无'
                # 价格
                prices = hotel.xpath(
                    'li[@class="hotel_price_icon"]/div[1]/div[@class="hotel_price"]/a/span[@class="J_price_lowList"]/text()')
                if prices:
                    price = prices[0].replace(' ', '').replace('\r\n', '')
                else:
                    price = u'无'
                # 礼品卡支付
                gift_cards = hotel.xpath(
                    'li[@class="hotel_price_icon"]/div[@class="original_price"]/span[@class="ticket_available"]/@title')
                if gift_cards:
                    gift_card = gift_cards[0].replace(' ', '').replace('\r\n', '')
                else:
                    gift_card = u'无'
                # 标签
                marks = hotel.xpath(
                    'li[@class="searchresult_info_name"]/p[@class="medal_list"]/span[@class="special_label"]/i/text()')
                if marks:
                    mark = '|'.join(marks)
                else:
                    mark = u'无'
                # 服务
                services = hotel.xpath('li[@class="searchresult_info_name"]/div[@class="icon_list"]/i/@title')
                if services:
                    service = '|'.join(services)
                else:
                    service = u'无'

                crawl_date = datetime.date.today()

                sql = (
                          "insert ignore into "
                          "20170119_xiecheng_hotel_url"
                          "(url, page_url, city_name, city_id, district_name, hotel_name, hotel_url, hotel_id, region, address, img, "
                          "star, mark, score, recommend, comment_num, price, gift_card, service, crawl_date) "
                          "values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s')"
                      ) % (
                          url, target_url, city_name, city_id, district_name, hotel_name, hotel_url, hotel_id, region, address, img,
                          star, mark, score, recommend, comment_num, price, gift_card, service, crawl_date
                      )
                print sql
                try:
                    mysql_dao.execute(sql)
                except:
                    print('error')
        else:
            print(req.status_code)
        page = page - 1


if __name__ == '__main__':
    city_list = City.city_list
    city_hotel_list = City.city_hotel_pingyin
    district = City.district
    for (city_name, city_id) in city_list.items():
        city_url = city_hotel_list[city_name]
        #http://hotels.ctrip.com/hotel/wuxi13
        for (district_name, district_id) in district[city_name].items():
            #http://hotels.ctrip.com/hotel/wuxi13location-500
            url = city_url + 'location-' + district_id
            print url
            last_page = get_last_page(url)
            try:
                get_hotel_url(url, last_page, city_name, city_id, district_name)
            except Exception as e:
                traceback.print_exc()
                print(e)


