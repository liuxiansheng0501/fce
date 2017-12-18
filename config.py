#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/14 19:41
# @Author  : liulijun
# @Site    : 
# @File    : config.py
# @Software: PyCharm

COM_NAME=['齿轮箱', '发电机', '变桨', '变频器', '偏航', '液压', '转速', '振动']
#评价等级
STATUS_LEVEL=['A', 'B', 'C', 'D','E']
#隶属度参数
M_a,M_b,M_c,M_d=0.07,0.42,0.72,0.89
#常权数
WEIGHT_CONST=0.5
#取时间平均值参数
# TABLE_MINS_ARGV={'fce_3mw':'600','fce_3mw_1min':'60','fce_3mw_10sec':'10'}
TABLE_MINS_ARGV={'fce_3mw_10sec':'10'}
#变权参数
DELTA_ARGV=0.1
ANALYSOR= '刘利军'
#远程数据库账号密码
REMOTE_DB = {'_user': 'llj', '_passwd': 'llj@2016'}
#本地数据库账号密码
LOCAL_DB = {'_user': 'root','_passwd': '911220'}
START_TIME= "2017-11-11 00:10:00"
END_TIME= "2017-11-11 00:00:00"
# 是否保存评估结果到远程数据库
TOREMOTE_DB=True