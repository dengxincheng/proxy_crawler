#!/usr/bin/env python
# encoding:utf-8-sig
"""
@version:
@author: 小邓
@file: run_spider.py
@time: 2016/9/8 0008 11:20
该文件为脚本执行文件
"""
import main_crawler,sys,csv,re,codecs
import sqlite3,datetime
import logging
import logging.config
import os
#数据输出路径
PATH_DATA = ""#"/home/ubuntu/storagenode/proxy_ips/"
#代码执行路径
PATH_ROOT = ""#"/home/ubuntu/storagenode/dengxincheng/proxy_crawler/"


def target_init(target_file):
    target_dic ={}
    reg = re.compile(r"/.(.+\..{2,3})/")
    for line in target_file:
        if line.strip()!='':
            target_name=reg.findall(line)[0]
            target_dic[line.strip()]=target_name
    return target_dic

def save2csv(data,filename='result.csv'):
    with codecs.open(PATH_DATA+datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")+filename,'ab+','utf_8_sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)
    print  '已保存csv！'

def save2db(data):
    conn = sqlite3.connect(PATH_ROOT+'proxy_ip.db')
    conn.text_factory = str
    for row in data:
        try:
            print row
            conn.execute('INSERT into ip VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row)
        except Exception, e:
            print e
            continue
    conn.commit()
    print '已更新数据！'

def main():
    getThreads = []
    checkThreads =[]
    target_file = open(PATH_ROOT+'targetlist.csv','r')
    targets = target_init(target_file)
    ProxyCheck = main_crawler.ProxyCheck
    ProxyGet = main_crawler.ProxyGet
    for url,name in targets.items():
        t = ProxyGet(url,name)
        getThreads.append(t)

    for i in range(len(getThreads)):
        getThreads[i].start()

    for i in range(len(getThreads)):
        getThreads[i].join()
    print '检测中！！'
    rawProxyList = main_crawler.checked_proxy_lsit
    for i in range(1000):
        t = ProxyCheck(rawProxyList[((len(rawProxyList)+999)/1000) * i:((len(rawProxyList)+999)/1000) * (i+1)])
        checkThreads.append(t)

    for i in range(len(checkThreads)):
        checkThreads[i].start()

    for i in range(len(checkThreads)):
        checkThreads[i].join()
    print '检测完毕！'

    save2csv(main_crawler.checked_proxy_lsit)
    save2db(main_crawler.checked_proxy_lsit)
    # os.system('sudo cp -rf /home/ubuntu/storagenode/dengxincheng/proxy_crawler/proxy_ip.db /home/ubuntu/storagenode/dengxincheng/django_test/proxy_ip.db')
if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=PATH_ROOT+'crawler.log',
                        filemode='a+')
    reload(sys)
    sys.setdefaultencoding('utf_8_sig')
    main()
    
