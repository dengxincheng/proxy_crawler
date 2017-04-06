#!/usr/bin/env python
# encoding: utf-8

"""
@version: 
@author: 小邓
@file: checker.py.py
@time: 2016/9/27 0027 15:49
"""
import sqlite3,csv,os
import main_crawler
ProxyCheck = main_crawler.ProxyCheck
thread_list=[]
conn = sqlite3.connect('/home/ubuntu/storagenode/dengxincheng/proxy_crawler/proxy_ip.db')
cur = conn.cursor()
reader = cur.execute("SELECT * from ip ").fetchall()
proxy_list =[]
for i in reader:
    row = list(i)
    proxy_list.append(row)
for i in range(100):
    t = ProxyCheck(proxy_list[((len(proxy_list) + 99) / 100) * i:((len(proxy_list) + 99) / 100) * (i + 1)])
    thread_list.append(t)
for t in thread_list:
    t.start()
for t in thread_list:
    t.join()
# print proxy_list
try:
    for proxy in main_crawler.checked_proxy_lsit:
        cur.execute("UPDATE ip set protocol=?,status=?,delay=?,type=?,baidu=?,google=?,weibo=?,taobao=?,checktime=? where ip = ?",(proxy[2],proxy[4],proxy[5],proxy[6],proxy[7],proxy[8],proxy[9],proxy[10],proxy[12],proxy[0]))
except Exception,e:
    print e
conn.commit()
reader = cur.execute("SELECT * from ip").fetchall()
with open('/home/ubuntu/storagenode/dengxincheng/django_test/static/data/result.csv','wb+') as csv_file:
#with open('result.csv','w+') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerows(reader)
os.system('sudo cp -rf /home/ubuntu/storagenode/dengxincheng/proxy_crawler/proxy_ip.db /home/ubuntu/storagenode/dengxincheng/django_test/proxy_ip.db')

