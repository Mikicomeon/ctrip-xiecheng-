# -*- coding: utf-8 -*-


import sys
import os
import datetime
import requests
from lxml import etree
import simplejson
import traceback
from public.mysqlpooldao import MysqlDao
from public.headers import Headers
from public.redispooldao import RedisDao
from public.hero import Hero

reload(sys)
sys.setdefaultencoding('utf-8')

file_path = os.path.dirname(os.path.abspath(__file__))
mysql_dao = MysqlDao()
redis_dao = RedisDao()
hero = Hero(file_path)

redis_key = 'xiecheng:20170118_xiecheng_hotel_tradingarea_list_url'


def get_last_page(url):
    last_page = 1
    try:
        headers = Headers.get_headers()
        req = requests.get(url, headers=headers)
        if req.status_code == 200:
            req.encoding = 'utf8'
            html = req.text
            selector = etree.HTML(html)
            last_pages = selector.xpath('//div[@id="page_info"]/div[@class="c_page_list layoutfix"]/a[last()]/text()')
            if len(last_pages) > 0:
                last_page = int(last_pages[0])
    except Exception as e:
        traceback.print_exc()
        print(e)
    return last_page


def get_hotel_url(list_id, city_name,trading_area_name, list_url, last_page):
    page = last_page
    while True:
        if page <= 0:

            break
        target_url = list_url + 'p' + str(page)
        print target_url
        headers = Headers.get_headers()
        try:
            req = requests.get(target_url, headers=headers)
        except Exception as e:
            traceback.print_exc()
            print(e)
        if req.status_code == 200:
            req.encoding = 'utf8'
            html = req.text
            selector = etree.HTML(html)
            hotels = selector.xpath('//div[@class="searchresult_list searchresult_list2"]')
            # print hotels
            for hotel in hotels:
                hotel_name = hotel_url = region_detail_name = hotel_address = hotel_star = hotel_facilities = hotel_label = hotel_image = ''
                hotel_grading = hotel_comment_num = 0

                # 酒店名称
                hotel_names = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="pic_medal"]/div[@class="hotel_pic"]/a/@title')
                if len(hotel_names) > 0:
                    hotel_name = hotel_names[0].replace('"', '').replace(' ', '')
                    # print hotel_name

                # 酒店url
                hotel_urls = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="pic_medal"]/div[@class="hotel_pic"]/a/@href')
                if len(hotel_urls) > 0:
                    hotel_url = 'http://hotels.ctrip.com' + hotel_urls[0]
                    # print hotel_url

                # 商圈细致名称
                region_detail_names = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_name"]/p[@class="searchresult_htladdress"]/a/text()')
                if len(region_detail_names) > 0:
                    region_detail_name = region_detail_names[0].replace('"', '').replace(' ', '')
                    # print region_detail_name

                # 酒店地址
                hotel_addresses = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_name"]/p[@class="searchresult_htladdress"]/text()')
                if len(hotel_addresses) > 0:
                    hotel_address = hotel_addresses[0].replace('"', '').replace(' ', '').replace(u'【', '')
                    # print hotel_address

                # 酒店星级
                hotel_stars = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_name"]/p[@class="medal_list"]/span/@title')
                if len(hotel_stars) > 0:
                    hotel_star = hotel_stars[0]
                    # print hotel_star

                # 酒店设施
                facilities_list = []
                facilities = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_name"]/div[@class="icon_list"]/i/@title')

                if len(facilities) > 0:
                    for facility in facilities:
                        facility_single = facility

                        facilities_list.append(facility_single)
                sign = ","
                hotel_facilities = sign.join(facilities_list)
                # print hotel_facilities

                # 酒店标签
                hotelLabel_list = []
                hotel_labels = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_name"]/p[@class="medal_list"]/span[@class="special_label"]/i/text()')
                # print len(hotel_labels)
                if len(hotel_labels) > 0:
                    for hotel_label in hotel_labels:
                        hotel_label1 = hotel_label
                        # print hotel_label1
                        hotelLabel_list.append(hotel_label1)
                        # print hotelLabel_list
                hotel_label = ",".join(hotelLabel_list)
                # print hotel_label

                ###### 酒店评分
                hotel_gradings = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_judge "]/div[@class="searchresult_judge_box"]/a/span[@class="hotel_value"]/text()')
                if len(hotel_gradings) > 0:
                    hotel_grading = hotel_gradings[0]
                    # print hotel_grading

                ########酒店评论数
                hotel_comment_nums = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="searchresult_info_judge "]/div[@class="searchresult_judge_box"]/a/span[@class="hotel_judgement"]/text()')
                if len(hotel_comment_nums) > 0:
                    hotel_comment_num = hotel_comment_nums[0].replace(u'源自', '').replace(u'位住客点评', '')
                    # print hotel_comment_num

                #########酒店图片
                hotel_images = hotel.xpath(
                    'ul[@class="searchresult_info"]/li[@class="pic_medal"]/div[@class="hotel_pic"]/a/img/@src')
                if len(hotel_images) > 0:
                    hotel_image = hotel_images[0]
                    # print hotel_image

                crawl_date = datetime.date.today()

                values = (
                    hotel_name, hotel_url, city_name, region_detail_name, trading_area_name,hotel_address, hotel_star, hotel_facilities, hotel_label,
                    hotel_grading, hotel_comment_num,
                    hotel_image, crawl_date)

                sql = (
                          'INSERT ignore INTO'
                          ' `xiecheng_trading_hotel_list_info`'
                          ' (`hotel_name`, `hotel_url`, `city_name`, `region_detail_name`, `trading_area_name`,'
                          ' `hotel_address`, `hotel_star`, `hotel_facilities`, `hotel_label`, `hotel_grading`,'
                          ' `hotel_comment_num`, `hotel_image`, `crawl_date`)'
                          ' VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s",'
                          ' "%s", "%s", "%s", "%s", "%s","%s")'
                      ) % values
                print(sql)
                try:
                    mysql_dao.execute(sql)
                except:
                    print('error')
        else:
            print(req.status_code)
        page = page - 1


if __name__ == '__main__':
    while True:
        hotel_list_json = redis_dao.lpop(redis_key)
        if hotel_list_json is None:
            break
        hotel_list = simplejson.loads(hotel_list_json)
        list_id = hotel_list[0]
        city_name = hotel_list[1]
        trading_area_name = hotel_list[2]
        list_url = hotel_list[3]
        # print list_url
        last_page = get_last_page(list_url)
        try:
            get_hotel_url(list_id, city_name, trading_area_name, list_url, last_page)
        except Exception as e:
            traceback.print_exc()
            print(e)
            continue
        sql1 = 'UPDATE `xiecheng_trading_area_hotel` SET `status`="1" WHERE (`id`="%s")' % list_id
        print(sql1)
        mysql_dao.execute(sql1)
