# -*- coding: utf-8 -*-


import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import simplejson


from public.mysqlpooldao import MysqlDao
from public.redispooldao import RedisDao

redis_key = 'xiecheng:20170118_xiecheng_hotel_tradingarea_list_url'
mysql_dao = MysqlDao()
redis_dao = RedisDao()

if __name__ == '__main__':
    sql = 'SELECT * FROM `xiecheng_trading_area_hotel` WHERE `status`=0'
    hotel_lists = mysql_dao.execute(sql)
    for hotel_list in hotel_lists:
        hotel_list_json = simplejson.dumps(hotel_list)
        redis_dao.rpush(redis_key, hotel_list_json)
        print(hotel_list_json)
