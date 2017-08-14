#!/usr/bin/env python2
#-*-coding:utf-8-*-
from threading import Thread
import random
from Queue import Queue
import pymongo
import requests
from lxml import etree
import re
import urllib2
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time
import os

#代理函数,用爬的ip来访问想爬取的网站
def url_user_agent(url,head):
    if os.path.getsize('proxy')==0:
        #设置使用代理
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}

        url1 = 'http://www.xicidaili.com/nn/1'
        req = urllib2.Request(url1, headers=header)
        res = urllib2.urlopen(req).read()

        selector = etree.HTML(res)
        ips = selector.xpath('//td/text()')
        f = open("proxy", "w")

        for x in range(263, len(ips), 12):
            ip = ips[x]
            tds = ips[x + 1]
            ip_temp = ip + "\t" + tds + "\n"
            f.write(ip_temp)

        f = open("proxy")
        lines = f.readlines()
        proxys = []
        for i in range(0, len(lines)):
            ip = lines[i].strip("\n").split("\t")
            try:
                proxy_host = "http://" + ip[0] + ":" + ip[1]
            except:
                continue
            proxy_temp = {"http": proxy_host}
            proxys.append(proxy_temp)
        for proxy in proxys:
            try:
                proxy_support = urllib2.ProxyHandler(proxy)
                # opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler(debuglevel=1))
                opener = urllib2.build_opener(proxy_support)
                urllib2.install_opener(opener)

                i_headers = {
                    # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    # 'Accept-Encoding':'gzip, deflate, sdch, br',
                    # 'Accept-Language':'zh-CN,zh;q=0.8',
                    # 'Cache-Control':'max-age=0',
                    # 'Connection':'keep-alive',
                    # 'Host':'movie.douban.com',
                    # 'Upgrade-Insecure-Requests':'1',
                    'User-Agent':head}
                # session = requests.session()
                # req = session.get(url,proxies=proxy,headers=i_headers)
                req = urllib2.Request(url, headers=i_headers)
                res = urllib2.urlopen(req).read()
                print res
                return res
            except Exception, e:
                print proxy
                print e
                continue
    else:
        f = open("proxy")
        lines = f.readlines()
        proxys = []
        for i in range(0, len(lines)):
            ip = lines[i].strip("\n").split("\t")
            try:
                proxy_host = "http://" + ip[0] + ":" + ip[1]
            except:
                continue
            proxy_temp = {"http": proxy_host}
            proxys.append(proxy_temp)
        for proxy in proxys:
            try:
                proxy_support = urllib2.ProxyHandler(proxy)
                # opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler(debuglevel=1))
                opener = urllib2.build_opener(proxy_support)
                urllib2.install_opener(opener)

                i_headers = {
                    # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    # 'Accept-Encoding':'gzip, deflate, sdch, br',
                    # 'Accept-Language':'zh-CN,zh;q=0.8',
                    # 'Cache-Control':'max-age=0',
                    # 'Connection':'keep-alive',
                    # 'Host':'movie.douban.com',
                    # 'Upgrade-Insecure-Requests':'1',
                    'User-Agent': head}
                # session = requests.session()
                # req = session.get(url,proxies=proxy,headers=i_headers)
                req = urllib2.Request(url, headers=i_headers)
                res = urllib2.urlopen(req).read()
                print res
                return res
            except Exception, e:
                print proxy
                print e
                continue

queue = Queue(10)
flag=0

head = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.3 Safari/537.36'

html_list = []
for i in range(6):
    url = 'https://movie.douban.com/tag/%E7%88%B1%E6%83%85?start=' + str(i*20)
    html_list.append(url)

class ProducerThread(Thread):
    def run(self):
        global queue
        global flag
        while True:
            for list in html_list:
                new_url = url_user_agent(list, head)
                selector = etree.HTML(new_url)
                # print new_url
                url = []
                req = selector.xpath('//div[@class="pl2"]/a/@href')

                for r in req:
                    url.append(r)
                for u in url:
                    # u = str(u).replace('\\', '')
                    queue.put(u)
                    time.sleep(random.random())
                    print u
            if i>9:
                print "\033[1;31;40m%s\033[0m" % "获取url结束！"
                flag=1
                break


class ConsumerThread(Thread):
    def run(self):
        global queue
        global flag
        client = pymongo.MongoClient('localhost', 27017)
        db = client['DoubanDianying']
        collection = db['love']
        while True:
            item = {}
            url = queue.get()
            item['url']=url
            html = requests.get(url)
            # print html.text
            selector = etree.HTML(html.text)
            img_html = selector.xpath('//div[@id="mainpic"]/a[@class="nbgnbg"]/img/@src')
            img_html = str(img_html).replace('[', '').replace(']', '').replace('\'', '')
            item['img_html']=img_html
            # print img_html

            info = {}
            item["name"] = selector.xpath('//div[@id="content"]/h1/span/text()')
            item["director"] = selector.xpath('//div[@id="info"]/span[1]/span[2]/a/text()')
            intro = selector.xpath('//div[@id="link-report"]/span/text()')
            intro_list = []
            for i in range(len(intro)):
                intro[i] = intro[i].replace(' ', '').replace('\n', '')
                intro_list.append(intro[i])
            item['intro'] = intro_list
            item['coutry'] = selector.xpath('//div[@id="info"]/text()[5]')
            item['score'] = selector.xpath('//div[@id="interest_sectl"]/div/div/strong/text()')
            item['scoreNum'] = selector.xpath(
                '//div[@id="interest_sectl"]/div/div/div/div[@class="rating_sum"]/a/span/text()')
            item['page'] = selector.xpath('//div[@id="info"]/span[@property="v:runtime"]/text()')
            item['page'] = selector.xpath('//div[@id="info"]/span[@property="v:runtime"]/text()')

            # item.setdefault('typeInfos',{})['name']={}
            # item.setdefault('typeInfos', {})['name1'] = {}
            # item.setdefault('typeInfos', {})['name11'] = {}
            # temp=item['typeInfos']
            # info['name']=selector.xpath('//div[@id="info"]/span[@property="v:genre"]/text()')
            # name='name'
            # for i in info['name']:
            #     temp.setdefault(name,{})[name]=i
            #     name=name+'1'

            info['name'] = selector.xpath('//div[@id="info"]/span[@property="v:genre"]/text()')
            name = ''
            for i in info['name']:
                i = 'name:' + i
                name = name + '{' + i + '},'
                item['typeInfos'] = name
            name = name[0:len(name) - 1]
            result = '[' + name + ']'
            item['typeInfos'] = result

            # actor_list = re.findall('href="(.*?)" rel="v:starring"',u,re.S)
            actor_list = selector.xpath('//div[@id="info"]/span[@class="actor"]/span[@class="attrs"]/a/@href')
            # actor_list = selector.xpath('//div[@id="info"]/span[3]/span[2]/span[1]/a[1]/@href')
            # // *[ @ id = "info"] / span[3] / span[2] / span[1] / a
            actor_url = []
            actorInfos_list = []
            for i in actor_list:
                actor_url.append("https://movie.douban.com" + i)
            for j in actor_url:
                actor_content = requests.get(j).text
                actor_selector = etree.HTML(actor_content)
                actor_name = actor_selector.xpath('//div[@id="content"]/h1/text()')
                actor_coutry = actor_selector.xpath('//div[@id="headline"]/div[@class="info"]/ul/li[4]/text()')
                actor_birth = actor_selector.xpath('//div[@id="headline"]/div[@class="info"]/ul/li[3]/text()')
                try:
                    if len(actor_coutry) and len(actor_birth):
                        actorInfos = '[' + '{' + 'name:' + actor_name[0] + '}' + ',' + '{courty' + actor_coutry[
                            1].replace('\n', '').replace(' ', '') + '},' + '{birth' + actor_birth[1].replace('\n',
                                                                                                             '').replace(
                            ' ', '') + '}' + ']'
                    else:
                        actorInfos = '[' + '{' + 'name:' + actor_name[0] + '}' + ']'
                except:
                    continue
                actorInfos_list.append(actorInfos)
            item["actorInfos"] = actorInfos_list

            duanping_urllist = selector.xpath(
                '//div[@id="comments-section"]/div[@class="mod-hd"]/h2/span[@class="pl"]/a/@href')
            m_headers = {'User-Agent': head}
            session = requests.session()
            duanping_url = duanping_urllist[0]
            new_duanping_url = ''
            for d in duanping_url:
                new_duanping_url = new_duanping_url + d
            new_duanping_url = new_duanping_url.replace('status=P', '')
            req = session.get(new_duanping_url, headers=m_headers)
            duanping_selector = etree.HTML(req.text)
            # 把短评数量提取出来并转换成int类型
            pagenum = duanping_selector.xpath(
                '//div[@id="content"]/div/div/div/ul[@class="fleft CommentTabs"]/li[@class="is-active"]/span/text()')
            new_pagenum = ''
            for p in pagenum:
                new_pagenum = new_pagenum + p
            new_pagenum = new_pagenum.replace('看过(', '').replace(')', '')
            try:
                new_pagenum = int(new_pagenum)
            except:
                continue
            # 首先把短评首页的内容爬取出来
            movieComments_created = duanping_selector.xpath(
                '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/span[@class="comment-time "]/text()')
            movieComments_content = duanping_selector.xpath('//div[@id="comments"]/div/div/p/text()')
            for i in range(len(movieComments_content)):
                try:
                    if len(movieComments_content[i].split()) == 0:
                        del movieComments_content[i]
                except:
                    break
            movieComments_score = duanping_selector.xpath(
                '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/span[2]/@class')
            userInfo_name = duanping_selector.xpath(
                '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/a/text()')
            userInfo_img = duanping_selector.xpath('//div[@id="comments"]/div/div/a/img/@src')
            movieComments_list = []
            for i in range(20):
                movieComments = '[' + '{' + 'name:' + userInfo_name[
                    i] + '}' + ',' + '{' + 'password:' + '123456' + '}' + ',' + '{' + 'img:' + userInfo_img[
                                    i] + '}' + ',' + \
                                '{' + 'created:' + str(movieComments_created[i]).replace('\n', '').replace(' ',
                                                                                                           '') + '}' + ',' + '{' + 'content:' + \
                                str(movieComments_content[i]).replace('\n', '').replace(' ',
                                                                                        '') + '}' + ',' + '{' + 'score:' + \
                                str(movieComments_score[i]).replace('allstar', '').replace(' rating', '') + '}' + ']'
                movieComments_list.append(movieComments)
            # userInfo_list=[]
            # for i in range(20):
            #     userInfo='['+'{'+'name:'+userInfo_name[i]+'}'+','+'{'+'password:'+'123456'+'}'+'{'+'img:'+userInfo_img[i]+'}'+']'
            #     userInfo_list.append(userInfo)
            # 爬取后面几页的短评内容
            for i in range(20, new_pagenum, 21):
                duanping_url = new_duanping_url + 'start=' + str(i)
                req = session.get(duanping_url, headers=m_headers)
                # f = requests.get(duanping_url,headers=m_headers)
                duanping_selector = etree.HTML(req.text)

                movieComments_created = duanping_selector.xpath(
                    '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/span[@class="comment-time "]/text()')
                movieComments_content = duanping_selector.xpath('//div[@id="comments"]/div/div/p/text()')
                for i in range(len(movieComments_content)):
                    try:
                        if len(movieComments_content[i].split()) == 0:
                            del movieComments_content[i]
                    except:
                        break
                movieComments_score = duanping_selector.xpath(
                    '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/span[2]/@class')
                userInfo_name = duanping_selector.xpath(
                    '//div[@id="comments"]/div/div/h3/span[@class="comment-info"]/a/text()')
                userInfo_img = duanping_selector.xpath('//div[@id="comments"]/div/div/a/img/@src')
                try:
                    for i in range(20):
                        movieComments = '[' + '{' + 'name:' + userInfo_name[
                            i] + '}' + ',' + '{' + 'password:' + '123456' + '}' + ',' + '{' + 'img:' + userInfo_img[
                                            i] + '}' + ',' + \
                                        '{' + 'created:' + str(movieComments_created[i]).replace('\n', '').replace(' ',
                                                                                                                   '') + '}' + ',' + '{' + 'content:' + \
                                        str(movieComments_content[i]).replace('\n', '').replace(' ',
                                                                                                '') + '}' + ',' + '{' + 'score:' + \
                                        str(movieComments_score[i]).replace('allstar', '').replace(' rating',
                                                                                                   '') + '}' + ']'
                        movieComments_list.append(movieComments)
                except:
                    break
                    # try:
                    #     for i in range(20):
                    #         userInfo = '[' + '{' + 'name:' + userInfo_name[i] + '}' + ',' + '{' + 'password:' + '123456' + '}' + '{' + 'img:' + \
                    #                    userInfo_img[i] + '}' + ']'
                    #         userInfo_list.append(userInfo)
                    # except:
                    #     break
            item["duanping"] = movieComments_list

            # f.write('name:' + '\n')
            # for data in item['name']:
            #     print data
            #     f.write(data + '\n')
            # f.write('director:' + '\n')
            # for data in item['director']:
            #     print data
            #     f.write(data + '\n')
            # f.write('actorInfos:' + '\n')
            # for data in item["actorInfos"]:
            #     print data
            #     f.write(data + '\n')
            # f.write('intro:' + '\n')
            # for data in item['intro']:
            #     print data
            #     f.write(data + '\n')
            # f.write('coutry:' + '\n')
            # for data in item['coutry']:
            #     print data
            #     f.write(data + '\n')
            # f.write('score:' + '\n')
            # for data in item['score']:
            #     print data
            #     f.write(data + '\n')
            # f.write('scoreNum:' + '\n')
            # for data in item['scoreNum']:
            #     print data
            #     f.write(data + '\n')
            # f.write('page:' + '\n')
            # for data in item['page']:
            #     print data
            #     f.write(data + '\n')
            # f.write('typeInfos:' + '\n')
            # print item['typeInfos']
            # f.write(item['typeInfos'] + '\n')
            # f.write('duanping:' + '\n')
            # for data in item['duanping']:
            #     print data
            #     f.write(data + '\n')
            time.sleep(10)
            queue.task_done()

            collection.insert_one(item)
            time.sleep(random.random())
            if queue.empty():
                if flag>0:
                    print "\033[1;31;40m%s\033[0m" % "解析结束！"
                    break

ProducerThread().start()
ConsumerThread().start()


