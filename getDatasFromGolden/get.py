#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 11:40
# @Author  : liulijun
# @Site    : https://github.com/markliu666/golden
# @File    : get.py
# @Software: PyCharm
import os
import pandas as pd

def OneWtgsWithMultiTags(classpath="./Lib/my.golden.jar",host="192.168.0.37",port=6327,user="mywind",passwd="MyData@2018",wtgs_id='30002001', tag_list=['giwindturbineoperationmode','grgridactivepower'], start_time="2017-10-01 00:00:00", end_time="2017-10-01 00:01:00"):#查数据
    os.environ['CLASSPATH'] = classpath
    from jnius import autoclass
    assert type(wtgs_id)==str,'请输入字符型机组号, as \'10001001\'！'
    assert len(wtgs_id) == 8, '请输入8位的机组号！'
    assert type(tag_list) == list, '请输入列表型标签点，as [\'a\',\'b\']！'
    assert start_time<=end_time, '查询开始时间大于结束时间！'
    server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
    server = server_impl(host, port, user, passwd) # 登录
    base_class = autoclass('com.rtdb.service.impl.BaseImpl')
    base = base_class(server)
    historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
    his = historian_impl(server)
    data_sort_calss = autoclass('com.rtdb.enums.DataSort')
    condition_class = autoclass('com.rtdb.model.SearchCondition')
    condition = condition_class()
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    return_val={}
    return_val['real_time']=[str(time) for time in pd.date_range(start=start_time, end=end_time, freq='S')]
    for tag in tag_list:
        valuelist = []
        condition.setTagmask("*" + wtgs_id + "*" + tag)
        points_ids = base.search(condition, 200, data_sort_calss.SORT_BY_TAG)  # 根据表名 批量获取
        # print("*" + wtgs_id + "*" + tag,points_ids)
        try:
            if int(base.getTypes(points_ids)[0].getNum()) in [6,7,8,9]:#INT类型
                result = his.getIntInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
            else:#float类型
                result = his.getFloatInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
            if result.size() > 0:
                for i in range(result.size()):
                    r = result.get(i)
                    if r.getValue() is not None or r.getValue()!=[]:
                        valuelist.append(r.getValue()) # 存储值序列
                    else:
                        valuelist.append('')
                return_val[tag]=valuelist
        except:
            print("*" + wtgs_id + "*" + tag,'搜索的标签点不存在！')
        finally:
            pass
    return_val=pd.DataFrame.from_dict(return_val)
    return_val=return_val[['real_time']+tag_list]
    return_val.index=return_val['real_time']
    his.close()
    server.close()
    return return_val

def MultiWtgsWithOneTag(classpath="./Lib/my.golden.jar",host="192.168.0.37",port=6327,user="mywind",passwd="MyData@2018",wtgs_list=['30002001','30002002'], tag='giwindturbineoperationmode', start_time="2017-10-01 00:00:00", end_time="2017-10-01 00:01:00"):#查数据
    os.environ['CLASSPATH'] = classpath
    from jnius import autoclass
    assert type(wtgs_list)==list,'请输入列表型机组号, as [\'10001001\',\'10001002\']！'
    assert type(tag) == str, '请输入字符串型标签点名！'
    assert start_time<=end_time, '查询开始时间大于结束时间！'
    server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
    try:
        server = server_impl(host, port, user, passwd) # 登录
        base_class = autoclass('com.rtdb.service.impl.BaseImpl')
        base = base_class(server)
    except:
        print('登录信息不正确！')
    finally:
        pass
    historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
    his = historian_impl(server)
    data_sort_calss = autoclass('com.rtdb.enums.DataSort')
    condition_class = autoclass('com.rtdb.model.SearchCondition')
    condition = condition_class()
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    return_val={}
    return_val['real_time']=[str(time) for time in pd.date_range(start=start_time, end=end_time, freq='S')]
    for wtgs_id in wtgs_list:
        valuelist = []
        print("*" + wtgs_id + "*" + tag)
        condition.setTagmask("*" + wtgs_id + "*" + tag)
        points_ids = base.search(condition, 200, data_sort_calss.SORT_BY_TAG)  # 根据表名 批量获取
        try:
            if base.getTypes(points_ids)[0].getNum() in [6,7,8,9]:#INT类型
                result = his.getIntInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
            else:#float类型
                result = his.getFloatInterpoValues(int(points_ids[0]), count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
            if result.size() > 0:
                for i in range(result.size()):
                    r = result.get(i)
                    if r.getValue() is not None or r.getValue()!=[]:
                        valuelist.append(r.getValue()) # 存储值序列
                    else:
                        valuelist.append('')
                return_val[wtgs_id]=valuelist
        except:
            print("*" + wtgs_id + "*" + tag,'搜索的标签点不存在！')
        finally:
            pass
    return_val=pd.DataFrame.from_dict(return_val)
    return_val=return_val[['real_time']+wtgs_list]
    return_val.index=return_val['real_time']
    his.close()
    server.close()
    return return_val

if __name__=="__main__":
    res=OneWtgsWithMultiTags(classpath="../Lib/my.golden.jar",wtgs_id='30002002', tag_list=['giwindturbineoperationmode','grgridactivepower'], start_time="2017-10-01 00:00:00", end_time="2017-10-01 00:01:00")
    print(res)