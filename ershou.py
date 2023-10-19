#!/usr/bin/env python
# coding=utf-8
# author: Zeng YueTian
# 此代码仅供学习与交流，请勿用于商业用途。
# 获得指定城市的二手房数据gz

from lib.spider.ershou_spider import *
import pandas as pd

if __name__ == "__main__":

    spider = ErShouSpider(SPIDER_NAME)
    spider.start()

    '''
    mes_list= []

    
    url = 'https://gz.ke.com/ershoufang/108403559345.html'
    soup  = get_area_ershou_info_url(url)
    mes_list.append(totalInform(url))

    url ='https://gz.ke.com/ershoufang/108404922580.html'
    soup  = get_area_ershou_info_url(url)
    mes_list.append(totalInform(url))
    print(mes_list)
    DATA = pd.DataFrame(columns=['所在区域','小区名称','价格(万)','元/平方米','小区均价','标题描述','信息网址','房屋户型','建筑面积','户型结构','建筑类型','所在楼层','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯','挂牌时间','交易权属','上次交易','房屋用途','房屋年限','产权所属','抵押信息','房本备件','核心卖点','小区介绍','适宜人群','户型介绍','周边配套','税费解析','交通出行','户型特色','户型分间','30天看房记录','7天看房记录',])
    for i in range(0,len(mes_list)):
        data = pd.DataFrame(mes_list[i])
        DATA = pd.concat([DATA,data],axis=0)
    DATA.to_csv('test.csv')    

    mes_str  = ['房屋户型','建筑面积','户型结构','建筑类型','所在楼层','套内面积','房屋朝向','建筑结构','装修情况','梯户比例','配备电梯']
    house_elements = soup.find('div', class_="newwrap baseinform").find('div',class_='base').find_all('li',class_="")
    print(house_elements)
    mes1 = get_inform(house_elements, mes_str)
    print(mes1)
    '''
    '''
    url = 'https://gz.ke.com/ershoufang/108404883042.html'
    mes1 = totalInform(url)
    print(mes1)
    ershou = spider.get_area_ershou_info('gz','linhe')
    print(len(ershou))
    print(ershou)
    '''