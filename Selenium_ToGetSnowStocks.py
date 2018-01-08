# encoding: utf-8
"""
author=fenglelanya
learn more
"""
"""
本策略是对雪球股票评论数据的采集
"""
import pandas as pd
import numpy as np
from snownlp import SnowNLP as nlp
import pymssql,datetime
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
import threading,Queue,io,time,re,os
import multiprocessing,collections
class GetContentThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)


def func(num,list_manage):
    get=SNLPCheckStocks()
    return get.getRequstData(num,manageList_=list_manage)

class SNLPCheckStocks(object):

    def __init__(self):
        super(SNLPCheckStocks,self).__init__()
        ip_ = '172.18.57.***'
        ip_port="1**"
        profile=webdriver.FirefoxProfile()
        profile.set_preference('network.proxy.type',1) # 默认值0，就是直接连接；1是手工配制代理
        profile.set_preference('network.proxy.http',ip_)
        profile.set_preference('network.proxy.http_port',ip_port)
        profile.set_preference('network.proxy.ssl', ip_)
        profile.set_preference('network.proxy.ssl_port', ip_port)
        profile.set_preference('network.proxy.share_proxy_settings', True)
        # 对于localhost的不用代理，这里必须要配置，否则无法和 webdriver 通讯
        profile.set_preference('network.proxy.no_proxies_on', 'localhost,127.0.0.1')
        profile.set_preference('network.http.use-cache', False)
        user_agent='Mozilla/5.0 (Windows NT 6.3; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0'
        profile.set_preference("general.useragent.override", user_agent)
        profile.update_preferences()
        self.driver=webdriver.Firefox(profile)
        #self.driver=webdriver.PhantomJS(executable_path=r'C:\Python27\Scripts\phantomjs-2.1.1-windows\bin\phantomjs.exe')
        #self.driver.set_page_load_timeout(5)
        #self.driver.maximize_window() # 浏览器全屏显示
        #print u'端口号==',self.driver.firefox_profile.port
        self.dateTimeTextDict={}
        self.xueqiu_line='https://xueqiu.com'
        # 登陆数据库
        self.host='192.***.1.***'
        self.database='****' #'StockTradeData'
        self.user='****'
        self.pwd='*****'
        #w.start()
        #datas = w.wset("SectorConstituent", "date={};sectorId=a001010100000000".format(date.today().strftime('%Y-%m-%d')))
        # self.codes_list=datas.Data[1]
        self.codes_list =self.withGetCodes('allof_codes.csv')
        self.someCodes=self.withGetCodes('some_get_codes.csv')
        #w.close()
        #self.getRequstData()

    def withGetCodes(self,path):
        #获取本地的股票代码
        code_list=[n[0] for n in np.array(pd.read_csv(path))]
        return code_list

    def dbConnect(self):
        """lian接数据库"""
        if not self.database:
            raise (NameError,"No such database!")
        else:
            self.cnn=pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.database,charset="utf8")
            cur=self.cnn.cursor()
            if not cur:
                raise (NameError,"Can not connect database")
            else:
                return cur

    def execQuery(self,sql):
        """执行查询语句"""
        try:
            cur=self.dbConnect()
            if cur:
                cur.execute(sql)
                resList=cur.fetchall()
                # 查询完毕后关闭连接
                self.cnn.close()
                return resList
        except Exception,e:
            print u'sql语句==',sql
            print u'执行查询语句的时候报错了，e=',e

    def execNonQuery(self,sql):
        """执行非查询语句"""
        cur = self.dbConnect()
        try:
            if cur:
                """提交sql指令"""
                cur.execute(sql)
                self.cnn.commit()
                self.cnn.close()
        except Exception, e:
            print u'sql==',sql
            print u'执行非查询语句时出错了==>', e

    def getPageLine(self,driver,code,url_):
        """获取网页的地址"""
        # 将屏幕上滑4次
        #code=self.currentCode
        for n in range(1, 5):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        time.sleep(3)
        respon =driver.page_source
        soup = BS(respon, 'lxml')
        #print u'soup=====>',soup
        try:
            artcle_line = map(lambda x: self.xueqiu_line + x['href'], soup.findAll('a', {'class': 'fake-anchor'}))
            artcle_line=list(set(artcle_line))
            self.dateTimeTextDict[u'· 来自Android客户端']=''
            self.dateTimeTextDict[u'· 来自iPhone客户端'] = ''
            self.dateTimeTextDict[u'· 来自雪球'] = ''
            self.dateTimeTextDict[u'· 来自持仓盈亏'] = ''
            self.dateTimeTextDict[u'· 来自iPad客户端'] = ''
            self.dateTimeTextDict[u'· 来自研报'] = ''
            self.dateTimeTextDict[u' 实盘交易'] = ''
            self.dateTimeTextDict[u'· 来自新闻']=''
            self.dateTimeTextDict[u'· 来自公告']=''
            self.dateTimeTextDict[u' 访谈']=''
            self.dateTimeTextDict[u'今天']=datetime.date.today().strftime("%Y-%m-%d")[-5:]
            years = ['2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010','0000']
            try:
                status_list=soup.findAll('article',{'class':'timeline__item'})
            except:
                return
            #print u'status-list====',status_list
            refrence=str(url_.replace('https','http'))
            for eachData in status_list:
                longtext=eachData.findAll('div',{'class':'timeline__item__content timeline__item__content--longtext'})
                if not longtext:continue
                userName_c=eachData.findAll('a',{'class':'user-name'})[0].text
                try:
                    date_time_c = eachData.findAll('a', {'class': 'date-and-source'})[0].text
                    upTime = [re.sub(k, v, date_time_c) for k, v in self.dateTimeTextDict.items() if k in date_time_c]
                    upTime = str(datetime.date.today().year) + "-" + upTime[0]
                    #print 'upTime==', upTime
                except:
                    upTime='0000-00-00'
                try:
                    if upTime[:4] in years:
                        dateTime_c = upTime
                    else:
                        dateTime_c = str(datetime.date.today().year) + "-" + upTime
                except:
                    dateTime_c = '0000-00-00'
                try:
                    try:
                        zhankai=eachData.findAll('div', {'class': 'content content--detail'})
                    except:
                        zhankai=[]
                    if len(zhankai) <= 0:
                        data__c = eachData.findAll('div', {'class': "content content--description"})[0].text
                    else:
                        data__c=zhankai[0].text
                except:
                    data__c='0'
                data__c = re.sub('\'', "\"", data__c)
                try:
                    title__c = eachData.findAll('h3', {'class': 'timeline__item__title'})[0].text
                except:
                    title__c=data__c
                title__c = re.sub('\'', "\"", title__c)
                try:
                    zhuanFa__ = eachData.findAll('span')[0].text
                    zhuanFa_c = filter(str.isdigit, zhuanFa__.encode('gbk'))
                    if not zhuanFa_c.strip():
                        zhuanFa_c='0'
                    else:
                        zhuanFa_c=zhuanFa_c
                except:
                    zhuanFa_c='0'
                try:
                    pingLun__ = eachData.findAll('span')[1].text
                    pingLun_c = filter(str.isdigit, pingLun__.encode('gbk'))
                    if not pingLun_c.strip():
                        pingLun_c='0'
                    else:
                        pingLun_c=pingLun_c
                except:
                    pingLun_c='0'
                try:
                    zan__ = eachData.findAll('span')[2].text
                    zan_c = filter(str.isdigit,zan__.encode('gbk'))
                    if not zan_c.strip():
                        zan_c='0'
                except:
                    zan_c='0'
                """
                print u'获取到的数据====================================>'
                print 'code==',code
                print 'userName_c==',userName_c
                print 'title__c==',title__c
                print 'zhuanFa_c==',zhuanFa_c
                print 'zan_c==',zan_c
                print 'pingLun_c==',pingLun_c
                print 'data__c==',data__c
                print 'dateTime_c==',dateTime_c
                """
                sql = "insert into  SnowStockTalks([code],[author_Name],[title],[zhuanFa],[pingLun],[zan],[datas],[upTime]) values('%s','%s','%s','%s','%s','%s','%s','%s')" \
                      "" % (code, userName_c, title__c, zhuanFa_c, pingLun_c,zan_c,data__c, dateTime_c)
                sql_check="SELECT TOP (10) [code],[author_Name],[title],[zhuanFa],[pingLun],[zan],[datas],[upTime] FROM [LiangJingJun].[dbo].[SnowStockTalks] " \
                          "where code='%s' and author_Name='%s' and title='%s' and datas='%s'and upTime='%s' "%(code,userName_c,title__c,data__c,dateTime_c)
                sql_return=self.execQuery(sql_check.encode('utf-8'))
                if sql_return:
                    continue

                self.execNonQuery(sql.encode('utf-8'))  # 写入数据库

        except Exception,E:
            print u'写入数据库出错==',E
        #time.sleep(2)
        return artcle_line

    def getRequstData(self,num,manageList_):
        """爬虫获取网络上的数据"""
        print u'子进程 ID ==%d (%s)'%(os.getpid(),num)
        for co in self.codes_list:
            self.currentCode = co[:-3]
            #lock_.acquire() # 上锁， 使用lock来同步，来避免访问共享资源的冲突
            print u'当前code==',self.currentCode
            #print u'someCode==',self.someCodes
            if co in self.someCodes or self.currentCode in manageList_:continue
            if self.currentCode not in manageList_:
                manageList_.append(self.currentCode)
            else:continue
            print u'过滤后的code==', self.currentCode
            time.sleep(1)
            print u'子进程操作的所有合约代码==', manageList_
            #lock_.release() # 释放锁
            #p=list.index(self.currentCode) # code 在list中的索引
            self.line_list = []
            try:
                if self.currentCode[0] == "6":
                    self.url = r'https://xueqiu.com/S/SH{}'.format(self.currentCode)
                elif self.currentCode[0] == '0' or self.currentCode[0] == '3':
                    self.url = r'https://xueqiu.com/S/SZ{}'.format(self.currentCode)
                self.driver.get(self.url)
                for k in range(1,100): # 爬去前100页的数据
                    if k==1:
                        article_line=self.getPageLine(self.driver,self.currentCode,self.url)
                        self.line_list.extend(article_line)
                        print u'第%s页=='%k
                    else:
                        print u'第%s页=='%k
                        self.driver.refresh() # 刷新网页
                        time.sleep(3)
                        try:
                            input_=self.driver.find_element_by_xpath('//*[@id="app"]/div[2]/div[2]/div[8]/div[4]/input')
                        except Exception,e:
                            try:
                                input_ = self.driver.find_element_by_xpath('//*[@id="app"]/div[2]/div[2]/div[7]/div[4]/input') # //*[@id="app"]/div[2]/div[2]/div[7]/div[4]/input
                            except Exception,e:
                                print u'%s break退出--------------------,e=%s'%(current_code_,e)
                                break
                        input_.send_keys(k)
                        time.sleep(1)
                        input_.send_keys(Keys.ENTER) # 摁回车键
                        time.sleep(2)
                        article_line=self.getPageLine(self.driver,self.currentCode,self.url)
                        self.line_list.extend(article_line)
                self.line_list=list(set(self.line_list))
                for s in self.line_list:
                    self.driver.get(s)
                    first_page_data = self.driver.page_source
                    soup = BS(first_page_data, 'lxml')
                    current_code_=self.currentCode # 股票代码
                    try:
                        title_ = soup.select('#app > div.container.article__container > article > h1')[0].text
                        #print u'标题==',title_
                    except Exception,e:
                        title_='0'
                    try:
                        author_name=soup.select('#app > div.container.article__container > div.article__author > div.avatar__name > a')[0]['data-screenname']
                        #print u'作者==',author_name
                    except Exception,e:
                        author_name='0'
                    try:
                        upTime=soup.select('#app > div.container.article__container > div.article__author > div.avatar__subtitle > a')[0].text[3:]
                        dateTime_=str(datetime.date.today().year)+"-"+upTime
                    except Exception,e:
                        dateTime_='0000-00-00'
                    talk = soup.select(
                        '#app > div.container.article__container > div.article__meta > div > div.widget-meta__info')[0].text.split()
                    talk_list = [p for p in talk if re.sub("\D", "", p) != ""]  # 提取出字符串中的数字
                    try:
                        pepole_talks = talk_list[1]
                    except Exception, e:
                        pepole_talks = '0'
                    try:
                        zhuanFa = talk_list[0]
                    except Exception,e:
                        zhuanFa='0'
                    try:
                        zan = talk_list[2]
                    except Exception,e:
                        zan = '0'
                    datas=soup.select('#app > div.container.article__container > article > div')
                    result=datas[0].text
                    Tit = re.sub('\'', "\"", title_)
                    result = re.sub('\'', "\"", result)
                    sql = "insert into  SnowStockTalks([code],[author_Name],[title],[zhuanFa],[pingLun],[zan],[datas],[upTime]) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                        current_code_,author_name, Tit, zhuanFa, pepole_talks, zan, result,dateTime_)
                    sql_check = "SELECT TOP (10) [code],[author_Name],[title],[zhuanFa],[pingLun],[zan],[datas],[upTime] FROM [LiangJingJun].[dbo].[SnowStockTalks] " \
                                "where code='%s' and author_Name='%s' and title='%s' and datas='%s' order by upTime " % (
                        current_code_, author_name, Tit, result)
                    sql_return = self.execQuery(sql_check.encode('utf-8'))
                    if sql_return:
                        continue
                    #with open()
                    self.execNonQuery(sql.encode('utf-8'))  # 写入数据库
            except NoSuchElementException,e:
                print e
        self.closeDriver()


    def closeDriver(self):
        """关闭浏览器"""
        self.driver.close()
        self.driver.quit()


if __name__ == '__main__':
    print u'主进程 id=%s' % (os.getpid())
    """执行多进程"""
    que=multiprocessing.Queue()
    lock_=multiprocessing.Lock()
    manage=multiprocessing.Manager()
    manage_lock=manage.Lock()
    manage_list=manage.list()
    pro_N=5
    pool = multiprocessing.Pool(processes=pro_N)  # 设定进程的数量为3
    # 创建一个进程池pool，并设定进程的数量为3，arange(4)会相继产生四个对象[0, 1, 2, 3,4]，四个对象被提交到pool中，\
    # 因pool指定进程数为3，所以0、1、2会直接送到进程中执行，当其中一个执行完后才空出一个进程处理对象3或4
    for n in np.arange(pro_N):
        pool.apply_async(func, args=(n,manage_list,))
    print 'Waiting for all child Process done...'
    pool.close()
    pool.join()
    print 'All child Process is done'

    #sql.closeDriver()
    """
    #多进程处理
    start = time.time()
    pool = multiprocessing.Pool(5)
    list_pro=np.arange(6)
    for n in list_pro:
        pool.apply_async(mainFunc,[snlp,n])
    pool.close()# 禁止新增进程
    pool.join()
    print u'一共用时：' + str(time.time() - start)
    
    """
