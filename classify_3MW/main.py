#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/12 11:01
# @Author  : liulijun
# @Site    : 
# @File    : main.py
# @Software: PyCharm
from classify_3MW import config
import pandas as pd
import numpy as np
from pandas import DataFrame
from datetime import *
import datetime
import pymysql
import copy
import sqlite3
class Component:
    def __init__(self, comp_name, tag, deterioration_data, weight_data):
        self.name=comp_name
        self.tags = tag
        self.__key_tags__()
        self.deterioration_data =deterioration_data[self.tag_EN]
        self.weight_data=weight_data[self.tag_EN]
    def __calculate__(self):
        self.eva_res={}
        self.deterioration_data_name=copy.deepcopy(self.deterioration_data)
        self.deterioration_data_name['class']='A'
        for i in range(len(self.deterioration_data)):
            vw=np.array(self.weight_data.loc[self.weight_data.index[i]])
            vw=vw/sum(vw)
            ar=self.__arf__(self.deterioration_data.ix[i])
            res=np.dot(vw,ar)
            id = np.where(res == max(res))
            self.deterioration_data_name['class'].iloc[i] = config.STATUS_LEVEL[id[0][0]]
            self.eva_res[self.weight_data.index[i]]=config.STATUS_LEVEL[id[0][0]]
        self.deterioration_data_name.to_csv('D:/work/故障诊断/模糊评价/状态转换原因追溯/'+self.name+'.csv')

    def __key_tags__(self):

        self.itags={}
        self.tag_CH=[]
        self.tag_EN = []
        for i in range(len(self.tags[self.name])):
            if str(self.tags[self.name].iloc[i]) == '1.0' and self.tags['tag_EN'][i] not in ['wtid','real_time']:
                self.itags[self.tags.index[i]]=self.tags['tag_EN'][i]
                self.tag_CH.append(self.tags.index[i])
                self.tag_EN.append(self.tags['tag_EN'][i])

    def __arf__(self,list_value):

        res_matrix=[]
        for i in range(len(list_value)):
            ar1_value = self.__ar1f__(list_value[i], config.M_a, config.M_b)
            ar2_value = self.__ar2f__(list_value[i], config.M_a, config.M_b, config.M_c)
            ar3_value = self.__ar3f__(list_value[i], config.M_b, config.M_c, config.M_d)
            ar4_value = self.__ar4f__(list_value[i], config.M_c, config.M_d)
            res_matrix.append([ar1_value,ar2_value,ar3_value,ar4_value])
        ar_mat=np.array(res_matrix)
        return ar_mat

    def __ar1f__(self, x, a, b):

        # assess 1

        assert a <= b
        if x<=a:
            return 1
        elif x>a and x<=b:
            return (b-x)/(b-a)
        else:
            return 0

    def __ar2f__(self, x, a, b, c):

        # assess 2

        assert a <= b and b<=c
        if x<=a:
            return 0
        elif x>a and x<=b:
            return (x-a)/(b-a)
        elif x>b and x<=c:
            return (c - x) / (c - b)
        else:
            return 0

    def __ar3f__(self, x, b, c, d):

        # assess 3

        res=self.__ar2f__(x, b, c, d)
        return res

    def __ar4f__(self, x, c, d):

        # assess 4

        assert c <= d
        if x <= c:
            return 0
        elif x > c and x <= d:
            return (x - c) / (d - c)
        else:
            return 1

class Turbine:

    def __init__(self,db_path,start_time,end_time,author):

        self.eva_results = {}
        self.real_data = DataFrame()
        self.unit=config.COM_NAME
        self.farm_name=db_path['farm_name'].iloc[0]
        self.__key_tags__()
        self.db_path = db_path
        self.start_time = start_time
        self.end_time = end_time
        self.author = author
        self.__eva_process__()

    def __key_tags__(self):
        # fetch tag name(EN,CH) from .xlsx
        self.tag_set = {}

        self.tag = pd.read_excel("../config/tag/" + self.farm_name + ".xlsx", sheetname="sheet1")

        for unit in config.COM_NAME:
            for i in range(len(self.tag[unit])):
                if str(self.tag[unit].iloc[i]) == '1.0' and self.tag['tag_EN'][i] not in ['wtid','real_time']:
                    self.tag_set[self.tag.index[i]] = self.tag['tag_EN'][i]

    def __eva_process__(self):

        # evaluate process:
        # step1: query source data of each turbine base on multiprocess
        # step2: evaluate each unit one by one
        # step3: evaluate the status of turbine based on the results of eight units
        for id in range(len(self.db_path)):
            path=self.db_path.iloc[id].tolist()
            self.__query_real_data__(path)
            if len(self.real_data)==0:
                print(path, 'empty')
            else:
                self.__mins_avg_value__()
                self.__alpha_beta_cal__()
                self.__deterioration__cal__()
                self.__weight__()
                eva_res = {}
                self.export_res=[]
                for iunit_name in self.unit:# loop the unit

                    iunit_eva=Component(iunit_name, self.tag, self.deter_value, self.weight)
                    iunit_eva.__calculate__()

                    for key in iunit_eva.eva_res.keys():
                        if key not in eva_res.keys():
                            eva_res[key]=[iunit_eva.eva_res[key]]
                        else:
                            eva_res[key].append(iunit_eva.eva_res[key])
                for key in eva_res.keys():
                    self.export_res.append([path[0],int(path[1]),int(path[2]),key,
                                            eva_res[key][0],eva_res[key][1],eva_res[key][2],eva_res[key][3],
                                            eva_res[key][4],eva_res[key][5],eva_res[key][6],eva_res[key][7],
                                            max(eva_res[key]),datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),self.author])
                self.__export_mysql__(path)
    def __query_real_data__(self, path):
        # query original data of turbine from remote database
        query_field=''
        for i,key in zip(range(len(self.tag_set.keys())),list(self.tag_set.keys())):
            if i!=len(self.tag_set.keys())-1:
                query_field+=self.tag_set[key]+','
            else:
                query_field += self.tag_set[key]
        query_condition =''
        for i,key in zip(range(len(self.tag_set.keys())),self.tag_set.keys()):
            if i!=len(self.tag_set.keys())-1:
                query_condition+=self.tag_set[key]+' is not null and '
            else:
                query_condition += self.tag_set[key]+' is not null '
        sqlstr = "SELECT real_time," + query_field + " FROM " + path[6] + " WHERE " + query_condition + "AND real_time BETWEEN \'" + \
                 self.start_time+"\' AND \'" + self.end_time+"\' ORDER BY real_time "
        # print(sqlstr)
        (conn, cur) = __mysql_conn__(path[3], int(path[4]), config.REMOTE_DB['_user'], config.REMOTE_DB['_passwd'], path[5])
        self.real_data = pd.read_sql(sqlstr, con=conn)
        self.real_data.index=list(self.real_data['real_time'])
        self.real_data = self.real_data.drop('real_time', 1)
        conn.close()

    def __mins_avg_value__(self):

        # 生成时间戳序列
        timestampstr = []
        real_data_min_avg={}
        starttimestamp = datetime.datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
        endtimestmp = datetime.datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")

        timestamp = starttimestamp
        while timestamp <= endtimestmp:
            timestampstr.append(str(timestamp))
            timestamp += timedelta(minutes=config.MINS_ARGV)

        for mintimestamp in timestampstr:

            mintimestamp=datetime.datetime.strptime(mintimestamp, "%Y-%m-%d %H:%M:%S")

            ts1 = mintimestamp + timedelta(seconds=config.MINS_ARGV / 2 * 60)
            ts2 = mintimestamp - timedelta(seconds=config.MINS_ARGV / 2 * 60)

            ts1 = ts1.strftime('%Y-%m-%d %H:%M:%S')
            ts2 = ts2.strftime('%Y-%m-%d %H:%M:%S')

            # print(ts1,ts2)

            selectedata=self.real_data.loc[ts2:ts1]
            selectedata.dropna(axis=0)
            real_data_min_avg[mintimestamp]=list(selectedata.mean())

        self.tag_EN=list(self.real_data.columns)
        print(self.tag_EN)
        self.real_data = pd.DataFrame(real_data_min_avg, index=self.tag_EN)
        self.real_data=self.real_data.T

    def __alpha_beta_cal__(self):

        for key in self.tag_set.keys():

            key_EN=self.tag_set[key]
            if np.isnan(self.tag['alpha1'].loc[key]):
                # print(key)
                alpha1_v = min(self.real_data[key_EN]) - 0.1
                self.tag['alpha1'].loc[key]=alpha1_v
            if np.isnan(self.tag['alpha2'].loc[key]):
                # print(key)
                alpha2_v = min(self.real_data[key_EN]) + (max(self.real_data[key_EN]) - min(
                    self.real_data[key_EN])) * 1 / 4
                self.tag['alpha2'].loc[key] = alpha2_v
            if np.isnan(self.tag['beta2'].loc[key]):
                # print(key)
                beta2_v = min(self.real_data[key_EN]) + (max(self.real_data[key_EN]) - min(
                    self.real_data[key_EN])) * 3 / 4
                self.tag['beta2'].loc[key] = beta2_v
            if np.isnan(self.tag['beta1'].loc[key]):
                # print(key)
                beta1_v = max(self.real_data[key_EN]) + 0.1
                self.tag['beta1'].loc[key] = beta1_v

    def __deterioration__cal__(self):
        self.deter_value={}
        timelist=list(self.real_data.index)
        for i in range(len(self.real_data)):
            self.deter_value[timelist[i]]=[]
            for key in self.tag_set.keys():
                key_EN=self.tag_set[key]
                x_v=self.real_data[key_EN].iloc[i]
                type_v=int(self.tag['type'].loc[key])
                alpha1_v = self.tag['alpha1'].loc[key]
                alpha2_v = self.tag['alpha2'].loc[key]
                beta2_v = self.tag['beta2'].loc[key]
                beta1_v = self.tag['beta1'].loc[key]
                self.deter_value[timelist[i]].append(self.__deterioration_type__(type_v,x_v,alpha1_v,alpha2_v,beta2_v,beta1_v))
                # print(key,x_v,self.__deterioration_type__(type_v,x_v,alpha1_v,alpha2_v,beta2_v,beta1_v))
        # self.deter_value = pd.DataFrame(self.deter_value, index=list(self.tag_set.keys()))
        self.deter_value = pd.DataFrame(self.deter_value, index=self.tag_EN)
        self.deter_value=self.deter_value.T

    def __weight__(self):
        self.weight= config.WEIGHT_CONST * np.exp(self.deter_value * config.DELTA_ARGV)

    def __deterioration_type__(self,type,x,alpha1,alpha2,beta2,beta1):

        if int(type)==1:
            res=self.__deterioration_type1__(x,alpha1,beta1)
        elif int(type)==2:
            res = self.__deterioration_type2__(x, alpha1,alpha2,beta2,beta1)
        elif int(type)==3:
            res = self.__deterioration_type3__(x,alpha1,beta1)
        else:
            res=0
        return res

    def __deterioration_type1__(self,x,alpha1,beta1):

        # lower better

        assert alpha1 <= beta1

        if x<alpha1:
            return 0
        elif x>=alpha1 and x<=beta1:
            return (x-alpha1)/(beta1-alpha1)
        else:
            return 1

    def __deterioration_type2__(self,x,alpha1,alpha2,beta2,beta1):
        # middle better
        assert alpha1 <= alpha2 and alpha2<=beta2 and beta2<=beta1

        if x<alpha1:
            return 1
        elif x>=alpha1 and x<alpha2:
            return (x-alpha2)/(alpha1-alpha2)
        elif x>=alpha2 and x<beta2:
            return 0
        elif x>beta2 and x<=beta1:
            return (x - beta2) / (beta1 - beta2)
        else:
            return 1

    def __deterioration_type3__(self,x,alpha1,beta1):

        # larger better

        assert alpha1 <= beta1

        if x<alpha1:
            return 1
        elif x>=alpha1 and x<=beta1:
            return (x-beta1)/(alpha1-beta1)
        else:
            return 0

    def __export_mysql__(self,path):

        if len(self.export_res) > 0:

            import socket

            hostname = socket.gethostname()
            if hostname != 'DESKTOP-6RO9O74':
                # if run in my mobile pc, save data on mysql db, other than save data on sqlite
                print('run on mobile pc')
                (conn, cur) = __mysql_conn__('192.168.0.19', 3306, config.REMOTE_DB['_user'], config.REMOTE_DB['_passwd'], 'iot_wind')
                sqlstr = "INSERT IGNORE INTO fce_3mw (farm_name,farm_code,wtgs_id,time,gearbox,generator,pitch,converter,yaw,hydraulic,rotor_speed,vibration,turbine,eva_time,evaluator) VALUES "
                value = '('
            else:
                print('run on working pc')
                (conn, cur) = __sqlite_conn__()
                sqlstr = "INSERT INTO type_3mw_1min (farm_name,farm_code,wtgs_id,time,gearbox,generator,pitch,converter,yaw,hydraulic,rotor_speed,vibration,turbine,eva_time,evaluator) VALUES "
                value = '('

            for j in range(len(self.export_res)):
                item = self.export_res[j]
                for i in range(len(item)):
                    value += '\'' + str(item[i]) + '\''
                    if i != len(item) - 1:
                        value += ','
                    elif j != len(self.export_res) - 1:
                        value += '),('
                    else:
                        value += ');'
            sqlstr += value
            # print(sqlstr)
            try:
                cur.execute(sqlstr)
                conn.commit()
            except:
                print('duplicate entry')
                pass
            conn.close()
            print(path[0],'-',path[7], 'evaluate finished!')
        else:
            print(path[0],'-',path[7], 'result empty!')

        print('export finished!')


def __mysql_conn__(_host, _port, _user, _passwd, _db):

    # connect mysql db

    try:
        conn = pymysql.connect(
            host=_host,
            port=_port,
            user=_user,
            passwd=_passwd,
            db=_db,
            charset="utf8"
        )
        cur = conn.cursor()
        return conn, cur
    except:
        print("Could not connect to MySQL server.")
        exit(0)

def __sqlite_conn__():
    conn = sqlite3.connect('../DB/fce.db')
    cur = conn.cursor()
    return conn, cur
class main:
    def __init__(self,start_time,end_time,author):
        self.__farm_path__()
        self.start_time=start_time
        self.end_time = end_time
        self.author = author
        self.__eva__()
    def __eva__(self):
        for farm in self.cal_farm_table_path:
            Turbine(self.cal_farm_table_path[farm], self.start_time, self.end_time, self.author)
            break
    def __farm_path__(self):
        self.cal_farm_table_path = {}
        farm = pd.read_excel("../config/path/farm_list.xlsx",sheetname='Sheet1')
        self.cal_farm_list=farm[farm['is_cal']==1.0]['farm_name'].tolist()
        for farm_ch_name in self.cal_farm_list:
            farm_path = pd.read_excel("../config/path/" + farm_ch_name + ".xlsx",sheetname='Sheet1')
            farm_path.index = farm_path['wtgs_id'].tolist()
            self.cal_farm_table_path[farm_ch_name]=farm_path

if __name__=="__main__":
    main(config.START_TIME, config.END_TIME, config.ANALYSOR)