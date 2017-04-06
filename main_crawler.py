#!/usr/bin/env python
# encoding: utf-8
"""
@version: 
@author: 小邓
@file: main_crawler.py
@time: 2016/9/6 0006 10:37
 该文件为ip爬取类和代理ip可用性检测类
"""
import datetime
import json
import random
import re
import sys
import threading
import time
import urllib2
import qqwry_query
import logging
import logging.config
PATH_ROOT = ""#"/home/ubuntu/storagenode/dengxincheng/proxy_crawler/"
logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=PATH_ROOT+'crawler.log',
                        filemode='a+')
reload(sys)
sys.setdefaultencoding('utf_8_sig')
logging.info('crawler is running')
mutex = threading.Lock()
checked_proxy_lsit =[]
proxy_list = []
LOACL_IP = '106.120.243.146'#本地出口ip
Anonymous_check = 'http://121.42.215.209/proxy_info'
#获取代理的类
class ProxyGet(threading.Thread):
    def __init__(self,target_url,target_name=''):
        threading.Thread.__init__(self)
        self.url = target_url
        self.name = target_name
    def getProxy(self):
        print "目标网站："+self.url
        send_headers = {
            'User-Agent': 'Mozilla/4.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive'
        }
        time.sleep(random.randint(1,10))
        try:
            req = urllib2.Request(self.url,headers=send_headers)
            req = urllib2.urlopen(req,timeout=20).read()
        except Exception,e:
            logging.warning(self.url+"---"+str(e))
            print self.url+'爬取失败！',e
            return
        dr = re.compile(r'<[^>]+>',re.S)
        dd = dr.sub(' ',req).replace('\n','').replace('\r','')
        regex = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) *(\d{2,5})")
        mutex.acquire()
        for row in regex.findall(dd):
            row = list(row)
            query = qqwry_query.IPInfo(PATH_ROOT+'QQWry.DAT')
            addr = query.getIPAddr(row[0])
            row.extend(['HTTP',addr[0]+addr[1]])
            row.extend([''] * 11)
            row[11] = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")
            row[-1] = self.name
            if row[0] not in proxy_list:
                proxy_list.append(row[0])
                checked_proxy_lsit.append(list(row))
        print self.url+"爬取结束！"
        mutex.release()
    def run(self):
        self.getProxy()
#检验代理类
class ProxyCheck(threading.Thread):
    def __init__(self,proxyList,setting=None):
        threading.Thread.__init__(self)
        self.proxyList = proxyList
        self.testurl = 'http://js.passport.qihucdn.com/5.0.3.js'
        self.teststring = '7.qhmsg.com'
        self.timeout=5
    def checkProxy(self):#代理基本连通性检测
        cookies = urllib2.HTTPCookieProcessor()
        for proxy in self.proxyList:
            proxy[-3] = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")
            proxyHandler = urllib2.ProxyHandler({"http" : r'http://%s:%s' %(proxy[0],proxy[1])})
            opener=urllib2.build_opener(cookies,proxyHandler)
            opener.addheaders =[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36')]
            result = ''
            try:
                t1 = time.time()
                req = opener.open(self.testurl,timeout=self.timeout)
                result=req.read()
                t2 = time.time()-t1
            except Exception,e:
                proxy[4]='unavailable'
                proxy[5]='-1'
                for i in range(5):
                    proxy[6+i]='-1'
                checked_proxy_lsit.append(proxy)
                continue
            if self.teststring in result:
                proxy[4]='available'
                proxy[5]='%.3f'%t2
                try:
                    proxy[6]=self.anonymity_checker(opener,proxy[0])
                    t1=datetime.datetime.strptime(proxy[-4].encode('utf-8'),"%Y-%m-%d_%H_%M")
                    t2=datetime.datetime.strptime(proxy[-3].encode('utf-8'),"%Y-%m-%d_%H_%M")
                    proxy[-2]=str(t1-t2).replace(',','')
                    visit_result = self.BGWT_visit(opener)
                    if visit_result.count('ok')>0:
                        proxy[2]='HTTP'
                    for i in range(visit_result.__len__()):
                        proxy[7+i]=visit_result[i]
                except Exception,e:
                    print e,proxy[0],proxy[1]
            else:
                proxy[4]='unavailable'
                proxy[5]='-1'
                for i in range(5):
                    proxy[6+i]='-1'
            checked_proxy_lsit.append(proxy)

    def BGWT_visit(self,opener):#百度淘宝谷歌微博访问性检测
        result = []
        visit_list={"baidu.com":"http://www.baidu.com/",
                    "google.com":"http://google.com",
                    "taobao.com":"https://www.taobao.com/",
                    "weibo.com":"http://weibo.com/"}
        for str,url in visit_list.items():
            content = ''
            try:
                t1 = time.time()
                req = opener.open(url,timeout=5)
                content = req.read()
                t2 = time.time()
            except Exception,e:
                result.append(-1)
                continue
            if str in content:
                result.append('ok')
        return result

    def anonymity_checker(self,opener,ip):#匿名性检测
        try:
            data = opener.open(Anonymous_check,timeout=5)
            data = json.load(data)
            if data.has_key('HTTP_X_FORWARDED_FOR') or data.has_key('HTTP_VIA') or data.has_key(
                    'HTTP_PROXY_CONNECTION'):
                if data.has_key('HTTP_X_FORWARDED_FOR') and data['HTTP_X_FORWARDED_FOR'] == LOACL_IP:
                    return 'Transparent'
                else:
                    return 'Anonymous'
            else:
                if data['REMOTE_ADDR'] == ip:
                    return 'High Anonymous'
        except:
            return 'Unknow'

    def run(self):
        self.checkProxy()
