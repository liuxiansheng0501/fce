#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/24 23:53
# @Author  : liulijun
# @Site    : 
# @File    : curve.py
# @Software: PyCharm
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import datetime

def curve():
    [conn,cur]=__sqlite_conn__()
    sqlstr="SELECT farm_name,farm_code,wtgs_id,time,gearbox,generator,pitch,rotor_speed,turbine FROM type_3mw WHERE wtgs_id='30002001'"
    res=pd.read_sql(sqlstr,con=conn)
    res.index=[datetime.datetime.strptime(time,"%Y-%m-%d %H:%M:%S") for time in res['time'].tolist()]
    res=res.replace('A',4)
    res = res.replace('B', 3)
    res = res.replace('C', 2)
    res = res.replace('D', 1)
    print(res)
    ax=res['turbine'].plot()
    plt.yticks([1,2,3,4])
    ax.set_yticklabels(['D','C','B','A'])
    plt.title('turbine')
    plt.show()

def __sqlite_conn__():
    conn = sqlite3.connect('../DB/fce.db')
    cur = conn.cursor()
    return conn, cur

if __name__=="__main__":
    curve()