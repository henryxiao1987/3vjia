#!/usr/bin/python
# ! -*- coding:utf8 -*-
import json
import logging
import os
import re
import time

import requests
from functools import reduce

from utils.Tool import CheloExtendedLogger, retry, time_limit
from utils.pg_client import PgClient

from bs4 import BeautifulSoup

from utils.qiniu_api import Qiniu

qiniu = Qiniu()
pg_client = PgClient(host="10.10.23.20")

logging.setLoggerClass(CheloExtendedLogger)
vjia_logger = logging.getLogger("3vjia_spider")

base_url = os.path.split(os.path.realpath(__file__))[0]

district_spider = f'{base_url}/district_spider.txt'
if not os.path.exists(district_spider):
    with open(district_spider, 'w') as f:
        f.write(',')


class VJia(object):
    def __init__(self):
        self._all_city = self.all_city_id()

    def all_city_id(self):
        resp = requests.get('http://qhstatic.oss.aliyuncs.com/openapi/cities.json')
        data = [{it['name']: it['cityid'] for it in item['cities']} for item in resp.json() if item.get('cities')]
        return reduce(lambda x, y: dict(list(x.items()) + list(y.items())), data)

    def get_city_code(self, provice_code):
        resp = requests.get('https://www.3vjia.com/hx/Home/GetCityList?districtId=%s' % provice_code)
        return resp.json()

    def get_community_data(self, city):
        '''
        获取没有户型的数据
        :return:
        '''
        sql = '''
        select t1.building_id,t1.building_name,city_name from dw.public.ol_api_building_info t1
            left join
            dw.public.ol_api_house_type t2
            on t1.building_id = t2.building_id where t2.id is null and t1.city_name = '%s'
        '''

        community_data = pg_client.query(sql % city)
        return community_data

    def search_provice(self):
        with open(f'{base_url}/province.json', 'r') as f:
            province = json.loads(f.read())

        for p_item in province:
            cities = self.get_city_code(p_item['i'])
            for c_item in cities:
                all_count = 0
                city_code = c_item['DistrictId']
                city_name = c_item['DistrictName']
                vjia_logger.info(f'抓取城市{city_name},城市行政编码{city_code}')
                try:
                    all_count += self.search_district(city_code, city_name)
                except Exception as e:
                    vjia_logger.error(str(e))

                vjia_logger.info(f'{city_name}总共抓取{all_count}个户型图')

    def search_district(self, city_code, city_name):
        lost_data = self.get_community_data(city_name)
        sql = """INSERT INTO dw.public.ol_api_house_type (building_id,building_name,specname,srcarea,area,planpic,qijiapic)
                                             values (%s,%s,%s,%s,%s,%s,%s)"""
        n = 1
        vjia_logger.info(f'{city_name}共有{len(lost_data)}个小区')

        total = 0
        has_spider = []
        if os.path.exists(district_spider):
            with open(district_spider, 'r') as f:
                has_spider = f.read().split(',')

        for _id, district_name, city in lost_data:
            district_name = district_name.replace('&quot;', '').replace('&nbsp;', '').strip()
            if f'{city}-{district_name}' in has_spider:
                print(f'{n}--{city} {district_name} 已经抓取过')
                vjia_logger.info(f'{n}--{city} {district_name} 已经抓取过')
                n += 1
                continue

            insert_data_list = []

            try:
                self.process_district(_id, district_name, city, n, insert_data_list, city_code)
            except Exception as e:
                print(str(e))

            with open(district_spider, 'a') as f:
                f.write(f'{city}-{district_name},')

            if insert_data_list:
                pg_client.insertmany(sql, insert_data_list)
                total += len(insert_data_list)
                vjia_logger.info(f'{city}-{district_name}抓取{len(insert_data_list)}个户型图')

            time.sleep(0.01)
            n += 1

        return total

    def process_district(self, _id, district_name, city, n, insert_data_list, city_code, p=1):
        s_url = 'https://www.3vjia.com/hx/home/SearchResult?cityCode=%s&buildingName=%s&p=%s'
        response = self.process_request(s_url % (city_code, district_name, p))
        if response.status_code == 200:
            html = BeautifulSoup(response.text, 'lxml')
            if not html.select('div.pic-house__info > h2'):
                print(f'{n}--{city} {district_name} 没有找到该小区')
                vjia_logger.info(f'{n}--{city} {district_name} 没有找到该小区')
                return

            count = html.select('div.pic-house__info > h2')[0].text
            print(f'{n}--{city} {district_name} {count} 第{p}页')
            vjia_logger.info(f'{n}--{city} {district_name} {count} 第{p}页')

            self.parse_html(_id, html, insert_data_list)
            count_int = int(re.findall(r'\d+', count)[0])
            if p * 9 < count_int:
                self.process_district(_id, district_name, city, n, insert_data_list, city_code, p + 1)
        else:
            print(f'请求错误 {response.status_code} {response.text}')

    def parse_html(self, _id, html, insert_data_list):
        li_list = html.select('body > div.page > div.pic-house.w1180.clearfix > div.pic-house__info > ul > li')
        print(f'解析html页面,当前页面含有{len(li_list)}个户型图')
        iindex = 1
        for item in li_list:
            try:
                print(iindex)
                # https://img3.admin.3vjia.com//UpFile/C00000022/PMC/BuildingRoomModel/201806/3vj-lpsc001/c9e78e90d39b43089916deed40b5fecb.jpg
                img = 'https://img3.admin.3vjia.com' + item.select_one('a > div > img').attrs['data-img']
                sub_district_name = item.select_one('p.single__name > a > span').text
                sub_city = item.select_one('p.single__location > span.single__text.text-overflow > span').text
                huxing = item.select_one('p.single__type > span.single__style').text
                area = item.select_one('p.single__type > span:nth-of-type(2)').text.replace('m2','').strip()  # contents ['73 m', <sup>2</sup>]

                if img:
                    print('start push to qinniu...')
                    qijiapic = qiniu.put_data(key=img, url=img)
                    print('end push to qinniu...')
                    row = [_id, sub_district_name, huxing, area, area, img, qijiapic]
                    insert_data_list.append(row)

                iindex += 1
            except Exception as e:
                vjia_logger.error(str(e))

    @retry(3)
    @time_limit(5)
    def process_request(self, url):
        print(url)
        return requests.get(url)

if __name__ == '__main__':
    vjia = VJia()
    # print(vjia.all_city_id())
    # print(vjia.get_community_data())
    vjia.search_provice()
    # vjia.search_district()
