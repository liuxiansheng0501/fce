#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/14 19:41
# @Author  : liulijun
# @Site    : 
# @File    : config.py
# @Software: PyCharm

COM_NAME=['齿轮箱', '发电机', '变桨', '变频器', '偏航', '液压', '转速', '振动']
#评价等级
STATUS_LEVEL=['A', 'B', 'C', 'D']
#隶属度参数
M_a,M_b,M_c,M_d=0.45,0.63,0.85,0.95
#常权数
WEIGHT_CONST=0.5
#取时间平均值参数
MINS_ARGV=1
#变权参数
DELTA_ARGV=0.1
START_TIME= "2017-10-03 00:10:00"
END_TIME= "2017-10-25 00:30:00"
ANALYSOR= '刘利军'
#远程数据库账号密码
REMOTE_DB = {'_user': 'llj', '_passwd': 'llj@2016'}
#本地数据库账号密码
LOCAL_DB = {'_user': 'root','_passwd': '911220'}