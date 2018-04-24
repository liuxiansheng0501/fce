#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/12 11:01
# @Author  : liulijun
# @Site    : 
# @File    : main.py
# @Software: PyCharm

# TODO-增加输出各大部件的标签点的评估结果（A,B,C,D)

import copy
import sqlite3
from datetime import datetime,timedelta
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import pymysql
import conf
from multiprocessing import  Pool
from getDatasFromGolden import get
from sqlalchemy import create_engine

output_path="E:/work/PHM/模糊评价/验证/"

class Component: # 部件级别
    def __init__(self, table, comp_name, tag, deterioration_data, weight_data):
        self.table=table
        self.name=comp_name
        self.tags = tag
        self.key_tags()
        self.deterioration_data =deterioration_data[self.tag_EN]
        self.weight_data=weight_data[self.tag_EN]

    def calculate(self):
        self.eva_res={}
        self.deterioration_data_name=copy.deepcopy(self.deterioration_data)
        self.deterioration_data_name['status']='A'
        for i in range(len(self.deterioration_data)):
            vw=np.array(self.weight_data.loc[self.weight_data.index[i]])
            if sum(vw)!=0:
                vw=vw/sum(vw)
            ar=self.arf(self.deterioration_data.ix[i])
            res=np.dot(vw,ar)
            id = np.where(res == max(res))# 根据隶属度计算公式计算所得结果，得到最大值坐在ID，映射到状态向量相同位置即为该记录的评估值
            self.deterioration_data_name['status'].iloc[i] = conf.STATUS_LEVEL[id[0][0]]
            self.eva_res[self.weight_data.index[i]]= conf.STATUS_LEVEL[id[0][0]]
        # self.deterioration_data_name.to_csv(output_path+self.table+"/"+self.name+'.csv') # 导出中间结果

    def key_tags(self):
        self.itags={}
        self.tag_EN=[]
        for i in range(len(self.tags[self.name])):
            if str(self.tags[self.name].iloc[i]) == '1.0' and self.tags['tag_EN'][i] not in ['wtid','real_time','giWindTurbineOperationMode']:
                self.itags[self.tags.index[i]]=self.tags['tag_EN'][i]
                self.tag_EN.append(self.tags['tag_EN'][i])

    def arf(self, list_value):
        res_matrix=[]
        for i in range(len(list_value)):
            ar1_value = self.ar1f(list_value[i], conf.M_a, conf.M_b)
            ar2_value = self.ar2f(list_value[i], conf.M_a, conf.M_b, conf.M_c)
            ar3_value = self.ar3f(list_value[i], conf.M_b, conf.M_c, conf.M_d)
            ar4_value = self.ar4f(list_value[i], conf.M_c, conf.M_d)
            res_matrix.append([ar1_value,ar2_value,ar3_value,ar4_value])
        ar_mat=np.array(res_matrix)
        return ar_mat

    def ar1f(self, x, a, b):
        # assess 1
        assert a <= b
        if x<=a:
            return 1
        elif x>a and x<=b:
            return (b-x)/(b-a)
        else:
            return 0

    def ar2f(self, x, a, b, c):
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

    def ar3f(self, x, b, c, d):
        # assess 3
        res=self.ar2f(x, b, c, d)
        return res

    def ar4f(self, x, c, d):
        # assess 4
        assert c <= d
        if x <= c:
            return 0
        elif x > c and x <= d:
            return (x - c) / (d - c)
        else:
            return 1

class Turbine: # 整机
    def __init__(self, db_path):
        self.unitSet= conf.COM_NAME
        self.farm_name=db_path['farm_name'].iloc[0]
        self.wtgs_name=db_path['wtgs_name'].iloc[0]
        [self.tag,self.tag_set]=self.key_tags()
        self.db_path = db_path.iloc[0].tolist()
        [start_time,end_time] = StartEndTime(self.db_path[2])
        if start_time!=end_time:
            start_time=datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S') #str->datetime
            end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            while start_time<end_time:# 按照时间步长循环执行
                self.start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                self.end_time = (start_time+timedelta(seconds=1800)).strftime('%Y-%m-%d %H:%M:%S')
                print(str(self.db_path[2])+', query start, from:' +self.start_time+" to: "+self.end_time+" at: " +datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                self.real_data=self.query_real_data(self.db_path)  # 查询真实存储值
                for table, time_delta in conf.TABLE_MINS_ARGV.items(): # 循环计算各计算类型
                    self.table=table
                    self.time_delta=int(time_delta)
                    [self.run_data,self.stop_data] = self.mins_avg_value()  # 平均值
                    self.eva_process()
                start_time=start_time+timedelta(seconds=1800)
        else:
            pass

    def key_tags(self):
        # 从配置文件.xlsx读取标签点名(EN,CH)
        tag_set = {}
        if self.farm_name=='克旗121' and self.wtgs_name!='12#':
            tag = pd.read_excel("./config/tag/" + self.farm_name + ".xlsx", sheetname="others")
        elif self.farm_name=='克旗121' and self.wtgs_name=='12#':
            tag = pd.read_excel("./config/tag/" + self.farm_name + ".xlsx", sheetname="12#")
        else:
            tag = pd.read_excel("./config/tag/" + self.farm_name + ".xlsx", sheetname="sheet1")
        for unit in conf.COM_NAME:
            for i in range(len(tag[unit])):
                if str(tag[unit].iloc[i]) == '1.0' and tag['tag_EN'][i] not in ['wtid','real_time']:
                    tag_set[tag.index[i]] = tag['tag_EN'][i]
        return tag,tag_set

    def eva_process(self):
        # evaluate process:
        # step1: query source data of each turbine base on multiprocess
        # step2: evaluate each unit one by one
        # step3: evaluate the status of turbine based on the results of eight units
        if len(self.real_data)==0:
            print(self.db_path, 'empty')
        else:
            export_res = []
            if len(self.run_data) > 0:
                # self.alpha_beta_cal() # 计算alpha,beta值
                deter_value=self.deterioration()  #计算劣化度值
                weight=self.weight(deter_value)  # 计算权重
                eva_res = {}
                for iunit_name in self.unitSet:# loop the unit，部件名称，从8大部件中循环
                    iunit_eva=Component(self.table, iunit_name, self.tag, deter_value, weight)
                    iunit_eva.calculate()
                    for key in iunit_eva.eva_res.keys():
                        if key not in eva_res.keys():
                            eva_res[key]=[iunit_eva.eva_res[key]]
                        else:
                            eva_res[key].append(iunit_eva.eva_res[key])
                for key in eva_res.keys(): #key --timestamp
                    export_res.append([self.db_path[0],int(self.db_path[1]),int(self.db_path[2]),key,eva_res[key][0],eva_res[key][1],eva_res[key][2],eva_res[key][3],
                                            eva_res[key][4],eva_res[key][5],eva_res[key][6],eva_res[key][7],max(eva_res[key]),datetime.now().strftime('%Y-%m-%d %H:%M:%S'), conf.ANALYSOR])
            if len(self.stop_data)>0:
                for timestamp in self.stop_data.keys():
                    export_res.append([self.db_path[0], str(self.db_path[1]), str(self.db_path[2]), str(timestamp), 'E', 'E', 'E','E','E', 'E', 'E', 'E','E', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), conf.ANALYSOR])
            if len(export_res)>0:
                self.export2DB(export_res)

    def query_real_data(self, path):
        # query original data of turbine from remote database
        query_field=[]
        combinaFileds=[]
        for i,key in zip(range(len(self.tag_set.keys())),list(self.tag_set.keys())):
            if '/' not in self.tag_set[key] and '-' not in self.tag_set[key]:#非组合类指标
                query_field.append(self.tag_set[key])
            else: # 组合类指标
                combinaFileds.append(self.tag_set[key])
        real_data=get.OneWtgsWithMultiTags(wtgs_id=str(path[2]),tag_list=query_field,start_time=self.start_time,end_time=self.end_time)
        grConverterTorque=real_data['grConverterTorque'].tolist()
        for i in range(len(grConverterTorque)):
            if grConverterTorque[i]>10000:
                grConverterTorque[i]=grConverterTorque[i]/821.098
        grTorqueSetpoint = real_data['grTorqueSetpoint'].tolist()
        for i in range(len(grConverterTorque)):
            if grConverterTorque[i] > 10000:
                grConverterTorque[i] = grConverterTorque[i] / 821.098
        real_data['grConverterTorque']=grConverterTorque
        real_data['grTorqueSetpoint']=grTorqueSetpoint
        for filed in combinaFileds:
            if '/' in filed:
                filedlist=filed.split('/')
                real_data[filed]=list((real_data[filedlist[0]]/real_data[filedlist[1]]).values)
            elif '-' in filed and '|' in filed:#扭矩设定值
                filedlist = filed.split('-')
                real_data[filed] = [abs(item) for item in list((real_data[filedlist[0][1:]]-real_data[filedlist[1][:-1]]).values)]
        return real_data

    def mins_avg_value(self): # 平均值
        timestampstr = list(pd.date_range(start=self.start_time,end=self.end_time,freq=str(self.time_delta)+'S').strftime("%Y-%m-%d %H:%M:%S"))[1:]
        run_real_data_min_avg={}
        stop_real_data_min_avg = {}
        for mintimestamp in timestampstr:
            mintimestamp=datetime.strptime(mintimestamp, "%Y-%m-%d %H:%M:%S")
            ts1 = mintimestamp + timedelta(seconds=self.time_delta / 2)
            ts2 = mintimestamp - timedelta(seconds=self.time_delta / 2)
            ts1 = ts1.strftime('%Y-%m-%d %H:%M:%S')
            ts2 = ts2.strftime('%Y-%m-%d %H:%M:%S')
            selectedata=self.real_data.loc[ts2:ts1]
            # TODO-取平均值之前是否需要排除异常值
            selectedata = selectedata.fillna(0)  # grConverterTorque/grTorqueSetpoint分母为0时NaN替换为0
            selectedata = selectedata.dropna(axis=0)
            if int(selectedata.loc[mintimestamp.strftime('%Y-%m-%d %H:%M:%S'), 'giWindTurbineOperationMode']) in [12,13,14]:
                selectedata=selectedata[(selectedata['giWindTurbineOperationMode']==12) | (selectedata['giWindTurbineOperationMode']==13) | (selectedata['giWindTurbineOperationMode']==14)]
                run_real_data_min_avg[mintimestamp.strftime('%Y-%m-%d %H:%M:%S')]=list(selectedata.mean())
            else:
                stop_real_data_min_avg[mintimestamp.strftime('%Y-%m-%d %H:%M:%S')] = "E"
        self.tag_EN=list(self.real_data.columns)
        if len(run_real_data_min_avg)>0:
            run_real_data_min_avg = pd.DataFrame.from_dict(run_real_data_min_avg)
            run_real_data_min_avg = run_real_data_min_avg.T
            run_real_data_min_avg.columns=self.tag_EN[1:]
        return run_real_data_min_avg,stop_real_data_min_avg

    def alpha_beta_cal(self): # 若参数表没有提供alpha,beta，则根据该方法计算
        for key in self.tag_set.keys():
            key_EN=self.tag_set[key]
            if np.isnan(self.tag['alpha1'].loc[key]):
                alpha1_v = min(self.real_data[key_EN]) - 0.1
                self.tag['alpha1'].loc[key]=alpha1_v
            if np.isnan(self.tag['alpha2'].loc[key]):
                alpha2_v = min(self.real_data[key_EN]) + (max(self.real_data[key_EN]) - min(
                    self.real_data[key_EN])) * 1 / 4
                self.tag['alpha2'].loc[key] = alpha2_v
            if np.isnan(self.tag['beta2'].loc[key]):
                beta2_v = min(self.real_data[key_EN]) + (max(self.real_data[key_EN]) - min(
                    self.real_data[key_EN])) * 3 / 4
                self.tag['beta2'].loc[key] = beta2_v
            if np.isnan(self.tag['beta1'].loc[key]):
                beta1_v = max(self.real_data[key_EN]) + 0.1
                self.tag['beta1'].loc[key] = beta1_v

    def deterioration(self):
        # 裂化度计算函数
        deter_value={}
        timelist=list(self.run_data.index)
        for i in range(len(self.run_data)):
            deter_value[timelist[i]]=[]
            tag_EN_list = []
            for tag_CH in self.tag_set.keys():
                tag_EN=self.tag_set[tag_CH]
                if not np.isnan(self.tag['type'].loc[tag_CH]):
                    tag_EN_list.append(tag_EN)
                    x_v=self.run_data[tag_EN].iloc[i]
                    type_v=int(self.tag['type'].loc[tag_CH])
                    alpha1_v = self.tag['alpha1'].loc[tag_CH]
                    alpha2_v = self.tag['alpha2'].loc[tag_CH]
                    beta2_v = self.tag['beta2'].loc[tag_CH]
                    beta1_v = self.tag['beta1'].loc[tag_CH]
                    deter_value[timelist[i]].append(round(self.deterioration_type(type_v, x_v, alpha1_v, alpha2_v, beta2_v, beta1_v),4))
        deter_value = pd.DataFrame.from_dict(deter_value)
        deter_value=deter_value.T
        deter_value.columns=tag_EN_list
        return deter_value

    def weight(self,deter_value): # 变权
        # TODO-12.12-省去变权这一步骤，直接取裂化度值
        return deter_value
        # return conf.WEIGHT_CONST * np.exp(deter_value * conf.DELTA_ARGV)

    def deterioration_type(self, type, x, alpha1, alpha2, beta2, beta1):
        if int(type)==1:
            res=self.deterioration_type1(x, alpha1, beta1)
        elif int(type)==2:
            res = self.deterioration_type2(x, alpha1, alpha2, beta2, beta1)
        elif int(type)==3:
            res = self.deterioration_type3(x, alpha1, beta1)
        else:
            res=0
        return res

    def deterioration_type1(self, x, alpha1, beta1):
        # lower better
        assert alpha1 <= beta1
        if x<alpha1:
            return 0
        elif x>=alpha1 and x<=beta1:
            return (x-alpha1)/(beta1-alpha1)
        else:
            return 1

    def deterioration_type2(self, x, alpha1, alpha2, beta2, beta1):
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

    def deterioration_type3(self, x, alpha1, beta1):
        # larger better
        assert alpha1 <= beta1
        if x<alpha1:
            return 1
        elif x>=alpha1 and x<=beta1:
            return (x-beta1)/(alpha1-beta1)
        else:
            return 0

    def export2DB(self, export_res):
        if len(export_res) > 0:
            export_res=pd.DataFrame(export_res,columns=['farm_name','farm_code','wtgs_id','time','gearbox','generator','pitch','converter','yaw','hydraulic','rotor_speed','vibration','turbine','eva_time','evaluator'])
            try:
                engine = create_engine('mysql+pymysql://llj:llj@2016@192.168.0.19/iot_wind?charset=utf8')  # 用sqlalchemy创建引擎
                export_res.to_sql(self.table,con=engine,if_exists="append",index=False)
            except:
                print(self.db_path[0], ' ', self.db_path[7], ' ', self.start_time, ' ', self.end_time, self.table,' duplicate entry!', ' ', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                pass
        else:
            print(self.db_path[0],' ',self.db_path[7],' ', self.start_time,' ', self.end_time, self.table, ' result empty!',' ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def mysql_conn(_host, _port, _user, _passwd, _db):
    # 连接mysql数据库
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

def sqlite_conn():
    # 连接sqlite数据库
    conn = sqlite3.connect('./DB/fce.db')
    cur = conn.cursor()
    return conn, cur

class main:
    def __init__(self):
        [self.cal_farm_list,self.cal_farm_table_path]=self.farm_path()
        self.loop()

    def loop(self):
        p = Pool(8)
        for farm in self.cal_farm_table_path:
            for row in range(len(self.cal_farm_table_path[farm])):
                wtgs_path=self.cal_farm_table_path[farm][row:row + 1]
                p.apply_async(self.multiProcessTask, args=(wtgs_path,))
        p.close()
        p.join()

    def multiProcessTask(self,wtgs_path):
        Turbine(wtgs_path)

    def farm_path(self):
        cal_farm_table_path = {}
        farm = pd.read_excel("./config/path/farm_list.xlsx",sheetname='Sheet1')
        cal_farm_list=farm[farm['is_cal']==1.0]['farm_name'].tolist()
        for farm_ch_name in cal_farm_list:
            farm_path = pd.read_excel("./config/path/" + farm_ch_name + ".xlsx",sheetname='Sheet1')
            farm_path.index = farm_path['wtgs_id'].tolist()
            cal_farm_table_path[farm_ch_name]=farm_path
        return cal_farm_list,cal_farm_table_path

def StartEndTime(wtgs_id):
    start_time_list=[]
    conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind', charset="utf8")
    for table, time_delta in conf.TABLE_MINS_ARGV.items():  # 循环计算各计算类型
        sqlstr = "SELECT MAX(time) FROM " + table + " WHERE wtgs_id=\'" + str(wtgs_id) + "\'"
        latest_cal_time = pd.read_sql(sql=sqlstr, con=conn)
        if latest_cal_time['MAX(time)'].iloc[0] is None:
            continue
        else:
            start_time_list.append(latest_cal_time['MAX(time)'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'))
    conn.close()
    if len(start_time_list)>0:
        start_time=min(start_time_list)
    else:
        start_time = "2018-01-01 00:00:00"
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')[0:13] + ":00:00"
    if (datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')-datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S')).days>10:
        start_time=(datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S')-timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    # start_time = "2018-01-10 01:00:00"
    # end_time = "2018-01-10 01:30:00"
    return start_time,end_time

if __name__ == '__main__':
    main()
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', seconds=3600, replace_existing=True)
    try:
        scheduler.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')
