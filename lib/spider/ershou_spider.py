#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 此代码仅供学习与交流，请勿用于商业用途。
# 爬取二手房数据的爬虫派生类

import re
import threadpool
from bs4 import BeautifulSoup
from lib.item.ershou import *
from lib.zone.city import get_city
from lib.spider.base_spider import *
from lib.utility.date import *
from lib.utility.path import *
from lib.zone.area import *
from lib.utility.log import *
import lib.utility.version
import pandas as pd

class ErShouSpider(BaseSpider):
    def collect_area_ershou_data(self, city_name, area_name, fmt="csv"):
        """
        对于每个板块,获得这个板块下所有二手房的信息
        并且将这些信息写入文件保存
        :param city_name: 城市
        :param area_name: 板块
        :param fmt: 保存文件格式
        :return: None
        """
        district_name = area_dict.get(area_name, "")
        csv_file = self.today_path + "/{0}_{1}.csv".format(district_name, area_name)

        ershous = self.get_area_ershou_info(city_name, area_name)
            # 锁定，多线程读写
        if self.mutex.acquire(1):
            self.total_num += len(ershous)
            # 释放
            self.mutex.release()
        if fmt == "csv":
            DATA = pd.DataFrame(columns=['所在区域','小区名称','价格(万)','元/平方米','小区均价','标题描述','信息网址','房屋户型','建筑面积','户型结构','建筑类型','所在楼层','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯','挂牌时间','交易权属','上次交易','房屋用途','房屋年限','产权所属','抵押信息','房本备件','核心卖点','小区介绍','适宜人群','户型介绍','周边配套','税费解析','交通出行','户型特色','户型分间','30天看房记录','7天看房记录',])
            for i in range(0,len(ershous)):
                data = pd.DataFrame(ershous[i])
                DATA = pd.concat([DATA,data],axis=0)
            DATA.to_csv(csv_file)    

        print("Finish crawl area: " + area_name + ", save data to : " + csv_file)

    @staticmethod



    def get_area_ershou_info(city_name, area_name):
        """
        通过爬取页面获得城市指定版块的二手房信息
        :param city_name: 城市
        :param area_name: 版块
        :return: 二手房数据列表
        """
        total_page = 1
        district_name = area_dict.get(area_name, "")
        # 中文区县
        chinese_district = get_chinese_district(district_name)
        # 中文版块
        chinese_area = chinese_area_dict.get(area_name, "")

        ershou_list = list()
        page = 'http://{0}.{1}.com/ershoufang/{2}/'.format(city_name, SPIDER_NAME, area_name)
        print(page)  # 打印版块页面地址
        headers = create_headers()
        response = requests.get(page, timeout=10, headers=headers)
        html = response.content
        soup = BeautifulSoup(html, "lxml")

        # 获得总的页数，通过查找总页码的元素信息
        try:
            page_box = soup.find_all('div', class_='page-box')[0]
            matches = re.search('.*"totalPage":(\d+),.*', str(page_box))
            total_page = int(matches.group(1))
        except Exception as e:
            print("\tWarning: only find one page for {0}".format(area_name))
            print(e)

        # 从第一页开始,一直遍历到最后一页
        for num in range(1, total_page + 1):
            page = 'http://{0}.{1}.com/ershoufang/{2}/pg{3}'.format(city_name, SPIDER_NAME, area_name, num)
            print(page)  # 打印每一页的地址
            headers = create_headers()
            BaseSpider.random_delay()
            response = requests.get(page, timeout=10, headers=headers)
            html = response.content

            soup = BeautifulSoup(html, "lxml")
            # 获得有小区信息的panel
            house_elements = soup.find_all('li', class_="clear")
            detail_urls =[]

            for house_elem in house_elements:
                #print('house_elem:',house_elem)
                detail_url = house_elem.find('a')['href']
                detail_urls.append(detail_url)
            for url in detail_urls:
                mes1 = totalInform(url)
                ershou_list.append(mes1)
        print(f'Total records{len(ershou_list)} in {city_name},{district_name},{area_name}')
        return ershou_list

    def start(self):
        city = get_city()
        self.today_path = create_date_path("{0}/ershou".format(SPIDER_NAME), city, self.date_string)

        t1 = time.time()  # 开始计时

        # 获得城市有多少区列表, district: 区县
        districts = get_districts(city)
        print('City: {0}'.format(city))
        print('Districts: {0}'.format(districts))

        # 获得每个区的板块, area: 板块
        areas = list()
        for district in districts:
            areas_of_district = get_areas(city, district)
            print('{0}: Area list:  {1}'.format(district, areas_of_district))
            # 用list的extend方法,L1.extend(L2)，该方法将参数L2的全部元素添加到L1的尾部
            areas.extend(areas_of_district)
            # 使用一个字典来存储区县和板块的对应关系, 例如{'beicai': 'pudongxinqu', }
            for area in areas_of_district:
                area_dict[area] = district
        print("Area:", areas)
        print("District and areas:", area_dict)

        # 准备线程池用到的参数
        nones = [None for i in range(len(areas))]
        city_list = [city for i in range(len(areas))]
        args = zip(zip(city_list, areas), nones)
        # areas = areas[0: 1]   # For debugging

        # 针对每个板块写一个文件,启动一个线程来操作
        pool_size = thread_pool_size
        pool = threadpool.ThreadPool(pool_size)
        my_requests = threadpool.makeRequests(self.collect_area_ershou_data, args)
        [pool.putRequest(req) for req in my_requests]
        pool.wait()
        pool.dismissWorkers(pool_size, do_join=True)  # 完成后退出

        # 计时结束，统计结果
        t2 = time.time()
        print("Total crawl {0} areas.".format(len(areas)))
        print("Total cost {0} second to crawl {1} data items.".format(t2 - t1, self.total_num))

def get_area_ershou_info_url(url):
    headers = create_headers()
    response = requests.get(url, timeout=10, headers=headers)
    html = response.content
    soup = BeautifulSoup(html, "lxml")   
    return soup

def get_str(S:str,s1:str,s2:str):
    '''
    从字符串中截取两个标识字符之间的字符串
    '''
    i1 = S.find(s1)
    i2 = S[i1:-1].find(s2)
    if s2 =='':
       return S[int(i1+len(s1)):len(S)]
    else:
        return S[int(i1+len(s1)):int(i1+i2)]



def get_baseinform(soup):  
    '''
    获取房源基本信息('房屋户型','建筑面积','户型结构','建筑类型','所在楼层','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯')和交易信息('挂牌时间','交易权属','上次交易','房屋用途','房屋年限','产权所属','抵押信息','房本备件')
    ''' 
    mes_str  = ['房屋户型','建筑面积','户型结构','建筑类型','所在楼层','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯']
    mes = {}
    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='base').find_all('li',class_="")
    for i in range(0,len(house_elements)):
        house_element = re.sub(' +', '', house_elements[i].text.strip()) 
        house_element = house_element.replace('\n','')  #删除所有回车键
        for s in mes_str:
            if s in house_element:
                mes[s]=get_str(house_element,s,'')


    mes_str  = ['挂牌时间','交易权属','上次交易','房屋用途','房屋年限','产权所属','抵押信息','房本备件']
    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='transaction').find_all('li',class_="")
    for i in range(0,len(house_elements)):
        house_element = re.sub(' +', '', house_elements[i].text.strip()) 
        house_element = house_element.replace('\n','')  #删除所有回车键
        for s in mes_str:
            if s in house_element:
                mes[s]=get_str(house_element,s,'')


    return mes




def get_inform(house_elements, mes_str):  
    mes = {}
    for i in range(0,len(house_elements)):
        house_element = re.sub(' +', '', house_elements[i].text.strip()) 
        house_element = house_element.replace('\n','')  #删除所有回车键
        for s in mes_str:
            if s in house_element:
                mes[s]=get_str(house_element,s,'')
    return mes
def get_layout(house_elements):
    mes = []
    for i in range(0,len(house_elements)):
        house_element = re.sub(' +', '', house_elements[i].text.strip()) 
        house_element = house_element.replace('\n','|')  

        mes.append(house_element)
    return mes


def totalInform(url):
    #print(url)
    soup  = get_area_ershou_info_url(url)
    mes1 = {}
    mes_str  = ['房屋户型','建筑面积','户型结构','建筑类型','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯']
    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='base').find_all('li',class_="")
    mes1 = get_inform(house_elements, mes_str)



    mes_str  = ['挂牌时间','交易权属','上次交易','房屋用途','房屋年限','产权所属','抵押信息','房本备件']
    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='transaction').find_all('li',class_="")
    mes1.update(get_inform(house_elements, mes_str))


    mes_str  = ['核心卖点','小区介绍','适宜人群','户型介绍','周边配套','税费解析','交通出行']
    house_elements = soup.find_all('div', class_="baseattribute clear")
    mes1.update(get_inform(house_elements, mes_str))

    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='base').find_all('li',class_="oneline")
    mes1['所在楼层']=house_elements[0].text.strip().replace('\n','').replace(' ','').replace('所在楼层','').replace('咨询楼层','')


    mes_str  = ['小区名称']
    house_elements = soup.find_all('div', class_="communityName")
    mes = get_inform(house_elements, mes_str)
    mes1['小区名称']= mes['小区名称'].replace('地图','')

    mes_str  = ['所在区域']
    house_elements = soup.find_all('div', class_="areaName")
    mes1.update(get_inform(house_elements, mes_str))

    house_elements = soup.find('div', class_="price").find_all('span',class_='total')
    mes1['价格(万)']=house_elements[0].text.strip()

    house_elements = soup.find('div', class_="unitPrice").find_all('span',class_='unitPriceValue')
    mes1['元/平方米']=house_elements[0].text.strip()

    house_elements = soup.find('div', class_="title").find_all('h1',class_='main')
    mes1['标题描述']=house_elements[0].text.strip()

    house_elements = soup.find('div', class_="xiaoqu_info").find_all('span',class_='xiaoqu_main_info price_red')
    mes1['小区均价']=house_elements[0].text.strip()


    house_elements = soup.find_all('div', class_="count")
    mes1['7天看房记录']=house_elements[0].text.strip()

    house_elements = soup.find_all('div', class_="totalCount")
    mes1['30天看房记录']=house_elements[0].text.strip()

    house_elements = soup.find_all('div', class_="fr list")
    if len(house_elements)>0:
        mes1['户型特色'] =house_elements[0].text.strip().replace('\n','').replace(' ','')

    house_elements = soup.find_all('ul', id="layoutList")

    mes1['户型分间'] = get_layout(house_elements)
    mes1['信息网址']=url

    return mes1

if __name__ == '__main__':
    
    pass