# -*- coding: utf-8 -*-

import sys
import requests
from lxml import etree
from public.mysqlpooldao import MysqlDao
from public.city import City
from public.headers import Headers
from selenium import webdriver
import re

reload(sys)
sys.setdefaultencoding('utf-8')
mysql_dao = MysqlDao()


def get_tradingArea_info(city_surename,city_sureurl):
    headers = Headers.get_headers()
    res = requests.get(city_sureurl, headers=headers)
    res.encoding = 'utf8'
    wb_data = res.text
    selector = etree.HTML(wb_data)
    trading_areas = selector.xpath('//div[@class="sec_item sta_unfold"]/div[@class="sec_new"]')
    if len(trading_areas) > 0:
        trading_area = trading_areas[0]
        trading_area_firsts = trading_area.xpath('dl[@class="seo_hot"]')
        for trading_area_first in trading_area_firsts:
            trading_area_first_group = trading_area_first.xpath('dt/h5/text()')[0]
            aaa = re.findall(u'商圈', trading_area_first_group)
            if aaa:
                trading_true_second_areas = trading_area_first.xpath('dd/a')
                # print trading_true_second_areas
                for trading_true_second_area in trading_true_second_areas:
                    trading_true_area = trading_true_second_area.xpath('@title')[0].replace(u'附近酒店','')
                    trading_true_url = 'http://hotels.ctrip.com' + trading_true_second_area.xpath('@href')[0]
                    # print city_sureurl, city_surename, trading_true_area, trading_true_url
                # print city_surename,trading_true_area,trading_true_area_url
                    sql = (
                              'INSERT IGNORE INTO `xiecheng_trading_area_hotel`'
                              '(`city_name`,`trading_area`,`trading_area_url`)'
                              'VALUES ("%s","%s","%s")'
                          ) % (city_surename, trading_true_area, trading_true_url)
                    print sql
                    mysql_dao.execute(sql)


if __name__ == '__main__':
    city_list = City.city_list
    # print city_list
    headers = Headers.get_headers()
    url = 'http://hotels.ctrip.com/domestic-city-hotel.html'
    res = requests.get(url,headers=headers)
    res.encoding = 'utf8'
    wb_data = res.text
    selector = etree.HTML(wb_data)
    cities = selector.xpath('//*[@id="base_bd"]/dl/dd/a')
    for city in cities:
        city_singlenames = city.xpath('text()')
        city_singleurls = city.xpath('@href')
        if city_singlenames:
            city_singlename = city_singlenames[0]
        if city_singleurls:
            city_singleurl = city_singleurls[0]
        # print city_singlename,city_singleurl
        if city_singlename in city_list:
            city_surename = city_singlename
            city_sureurl = 'http://hotels.ctrip.com/' + city_singleurl
            print city_surename,city_sureurl
            get_tradingArea_info(city_surename,city_sureurl)




