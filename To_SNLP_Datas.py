# encoding: utf-8
"""
author=fenglelanya
learn more
"""
import jieba,pymssql,multiprocessing
from snownlp import sentiment
from snownlp.classification import bayes
from snownlp import  SnowNLP as nlp
import pandas as pd
import numpy as np
import time,datetime
import codecs,re
import sys,chardet
import tushare as ts
import requests as rq
from functools import partial
reload(sys)
sys.setdefaultencoding('utf-8')
def func(process,list_manage):
    get=SNLP_ToHandleData()
    return get.handleDatas(process_=process,manageList_=list_manage)

class SNLP_ToHandleData(object):
    def __init__(self):
        super(SNLP_ToHandleData,self).__init__()
        self.testPath='snowTestData.txt'
        self.changeTestPath = 'snowchangeEncodingTestData.txt'
        negPath='neg_jxl_line.txt'
        posPath='pos_jxl_line.txt'
        target_encoding = 'utf-8'
        self.host = '192.168.1.152'
        self.database = 'LiangJingJun'  # 'StockTradeData'
        self.user = 'Traders'
        self.pwd = 'abcd4321'
        self.allCodeListPath=r'allof_codes.csv'
        self.someCodeListPath = r'some_get_codes.csv'
        self.allCodeData=self.readCodeList(self.allCodeListPath)
        self.someCodeData = self.readCodeList(self.someCodeListPath)
        self.dateTimeTextDict={}
        self.dateTimeTextDict[u'· 来自Android客户端'] = ''
        self.dateTimeTextDict[u'· 来自iPad客户端'] = ''
        self.dateTimeTextDict[u'· 来自微信小程序'] = ''
        self.dateTimeTextDict[u'· 来自iPhone客户端'] = ''
        self.dateTimeTextDict[u'· 来自弹幕'] = ''
        self.dateTimeTextDict[u'· 来自雪球'] = ''
        self.dateTimeTextDict[u'· 来自雪球'] = ''
        self.dateTimeTextDict[u'· 来自分享按钮'] = ''
        self.dateTimeTextDict[u'· 来自分享按钮'] = ''
        self.dateTimeTextDict[u'· 来自乐视超级手机 1 pro'] = ''
        self.dateTimeTextDict[u'· 来自研报'] = ''
        self.dateTimeTextDict[u' 实盘交易'] = ''
        self.dateTimeTextDict[u'· 来自新闻'] = ''
        self.dateTimeTextDict[u'· 来自公告'] = ''
        self.dateTimeTextDict[u' 访谈'] = ''
        self.dateTimeTextDict[u'今天'] = ''
        self.dateTimeTextDict[u'小时前'] = ''
        self.dateTimeTextDict[u'分钟前'] = ''# · 来自乐视超级手机 1 pro
        self.dateTimeTextDict[u'昨天'] = ''
        self.dateTimeTextDict[u'今天'] = ''

        self.indexDateList = self.tuShareGetHist(code='0000001', IndexBool_=True)  # 获取上证指数的历史数据
        #print u'indexDateList==',self.indexDateList
        #self.trainSentimentCorpus(negPath=negPath,posPath=posPath,target_encoding=target_encoding)
        #self.handleDatas()
        #++++++++++++++++++++++++++++++++++++++++++++++++
        #读取测试集
        #titles,datas=self.handleDatas()#self.readFile(self.testPath) #返回标题数据和文章数据
        #print u'title==',titles
        #print u'datas==',datas
        #titleSentimentDic=self.checkDataNLP(titles)
        #datasSentimentDic =self.checkDataNLP(datas)
        #print 'titleSentimentDic==',titleSentimentDic
        #print 'datasSentimentDic==', datasSentimentDic
        # datas 的情感分析
    def tuShareGetHist(self,code,IndexBool_=None):
        """获取指定code历史行情"""
        date_=datetime.date.today().strftime("%Y%m%d")
        headers = {
            'Connection': 'Keep-Alive',
            'Accept': 'text/html, application/xhtml+xml, */*',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
        }
        api=r'http://quotes.money.163.com/service/chddata.html?code=%s&start=20170101&end=%s&fields=TOPEN'%(code,date_)
        data= rq.get(url=api,headers=headers).text.decode('utf-8') # 日期,股票代码,名称,开盘价
        data=data.split('\r\n')[1:]
        data=(x.replace("'",'') for x in data)
        data=[x.split(',') for x in data]
        data=data[0:-1]
        df=pd.DataFrame(data=data,columns=['tradeDate','code','name','openPrice']).sort_values(by=['tradeDate'])
        return df['tradeDate'].values

    def contentReplace(self,series):
        """去空格"""
        return [n.replace(' ','') for n in series.values] # 删除空行/多余的空格

    def filterDateTime(self,date_string):
        """处理非合法date格式:'2018-2017-01-01 10:19'==>'2017-01-01 10:19'"""
        yearList=['2013','2014','2015','2016','2017','2018','2019','2020']
        #print u'date_string==',date_string
        self.dateTimeDict = {}
        self.dateTimeDict[u'· 来自Android'] = ''
        self.dateTimeDict[u'· 来自iPhone'] = ''
        self.dateTimeDict[u'· 来自iPad'] = ''
        self.dateTimeDict[u' 客户端'] = ''
        self.dateTimeDict[u'· 来自乐视超级手机 1'] = ''
        returnData = [date_string.replace(k,v) for k, v in self.dateTimeDict.items() if k in date_string]
        if len(returnData) > 0:returnData_= returnData[0]
        else:returnData_= date_string
        returnData_ = [date_string.replace(k, v) for k, v in self.dateTimeDict.items() if k in returnData_]
        #print u'returData==',returnData_
        if len(returnData) > 0:returnData_= returnData[0]
        else:returnData_= date_string
        newData_=returnData_.strip()
        try:date_Last = datetime.datetime.strptime(newData_,"%Y-%m-%d %H:%M")
        except:
            try:
                date_Last = datetime.datetime.strptime(newData_,"%Y-%m-%d")
                #print u"%Y-%m-%d date_Last",date_Last
            except:
                if len(newData_)<12:return '0'
                else:date_Last = newData_[5:]
                try:
                    if(len(date_Last)>=16):
                        date_Last=datetime.datetime.strptime(date_Last,"%Y-%m-%d %H:%M")
                    else:pass
                except:
                    if (len(date_Last) >= 16):date_Last=date_Last[5:]
            """
            date_Last = newData_
                if len(date_Last)<10:pass
                else:date_Last = datetime.datetime.strptime(date_Last,"%Y-%m-%d %H:%M")
                print u'2try:-->date_Last==',date_Last
            """
        #print u'LastDate====>', date_Last
        date_Last=str(date_Last)
        try:return date_Last[:10].strip()
        except:return date_Last.strip()

    def secondFilterDate(self,txt):
        """过滤日期"""
        returnData = [txt.replace(k, v) for k, v in self.dateTimeDict.items() if k in txt]
        if len(returnData) > 0:
            returnData_ = returnData[0]
        else:
            returnData_ = txt
        if(len(returnData_)<10):
            return '0'
        else:
            try:date_Last = datetime.datetime.strptime(returnData_, "%Y-%m-%d")
            except:return '0'
            return str(date_Last)

    def checkDataNLP(self,factorDF=None,code_=None):
        """
        判断TXT的情感偏向
        :param self:
        :param txt: df
        :return: 返回TXT的情感偏向sentments
        """
        content_title = self.contentReplace(factorDF['title']) # 删除空行/多余的空格
        content_datas = self.contentReplace(factorDF['datas'])  # 删除空行/多余的空格
        # # title的情感色彩分析
        factorDF['title_Marking'] = map(lambda x: 0 if x == '0' or len(x) == 0 else(float(nlp(x).sentiments)), content_title)
        # # datas的情感色彩分析
        factorDF['datas_Marking'] = map(lambda x: 0 if x == '0' or len(x) == 0 else(float(nlp(x).sentiments)), content_datas)
        upTimeValues = factorDF['upTime'].values
        factorDF['upTime']=map(self.filterDateTime, upTimeValues)
        #print "factorDF['upTime']==",factorDF['upTime']
        factorDFNew=factorDF[['code', 'title_Marking', 'datas_Marking', 'upTime']]
        upTimeList=np.array(factorDFNew['upTime'].values)
        indexListNew = ['0' if len(n.strip()) < 10 else(n.strip()) for n in upTimeList]
        factorDFNew['upTime']=indexListNew
        # 开始groupby 汇总每一天的数据
        factorData_DF=pd.DataFrame(data=factorDFNew.values,columns=["code","titleMarketing","datasMarketing","dateUpTime"])
        factorData_DF['dateUpTime']=map(self.secondFilterDate,factorData_DF['dateUpTime'])
        factorDFLast = factorData_DF[factorData_DF['dateUpTime'] != '0']
        code_list = factorDFLast['code'].values
        factorDFLast.drop('code',axis=1,inplace=True)
        resultDF=pd.DataFrame(index=factorDFLast['dateUpTime'].values,data=factorDFLast.values,columns=['titleMarketing','datasMarketing','dateUpTime'])
        resultDF['titleMarketing']=map(lambda x:float(x),resultDF['titleMarketing'].values)
        resultDF['datasMarketing'] = map(lambda x: float(x), resultDF['datasMarketing'].values)
        resultDF.sort_values(by=['dateUpTime'], inplace=True)
        new_data=resultDF.groupby('dateUpTime').mean()
        newCount=len(new_data)
        new_data['code']=code_list[:newCount]
        new_data['dateUpTime']=new_data.index
        # 根据历史交易日，填充所有缺失数据
        lostDate = []  # 把所有缺失时间收集起来一起填充
        index_data=new_data.index
        for dt in self.indexDateList:
            if len(dt)<10:pass
            else:
                dt=dt+" 00:00:00"
                if dt in index_data:continue
                else:lostDate.append(dt)
        for dt in lostDate:new_data.loc[dt] = {'titleMarketing': 0,'datasMarketing':0,'code':code_,'dateUpTime':dt}
        new_data.sort_values(by=['dateUpTime'], inplace=True)
        new_data.set_index('dateUpTime')
        new_data['titleMarketing'] = map(lambda x: x if x > .5 else(0 if x == 0 else(x - .5)),new_data['titleMarketing'].values)
        new_data['datasMarketing'] = map(lambda x: x if x > .5 else(0 if x == 0 else(x - .5)),new_data['datasMarketing'].values)
        new_data['title_Rolling5_mean']=np.array(new_data['titleMarketing'].rolling(min_periods=1,window=5).mean().values)
        new_data['title_Rolling10_mean']=np.array(new_data['titleMarketing'].rolling(min_periods=1,window=10).mean().values)
        new_data['title_Rolling20_mean']=np.array(new_data['titleMarketing'].rolling(min_periods=1,window=20).mean().values)
        new_data['title_Rolling30_mean']=np.array(new_data['titleMarketing'].rolling(min_periods=1,window=30).mean().values)
        new_data['datas_Rolling5_mean']=np.array(new_data['datasMarketing'].rolling(min_periods=1,window=5).mean().values)
        new_data['datas_Rolling10_mean']=np.array(new_data['datasMarketing'].rolling(min_periods=1,window=10).mean().values)
        new_data['datas_Rolling20_mean']=np.array(new_data['datasMarketing'].rolling(min_periods=1,window=20).mean().values)
        new_data['datas_Rolling30_mean']=np.array(new_data['datasMarketing'].rolling(min_periods=1,window=30).mean().values)
        #date__=str(datetime.datetime.now()).replace(':','').replace('.','')
        #df.to_csv(r'{}_{}.csv'.format(columnName_,date__),encoding='utf_8_sig')
        #print u'写入完成'
        return new_data

    def readCodeList(self,path):
        """读取本地股票代码"""
        return codecs.open(path,'r',encoding='utf-8',errors='ignore').readlines()

    def dbConnect(self):
        """lian接数据库"""
        if not self.database:raise (NameError, "No such database!")
        else:
            cnn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.database,
                                  charset="utf8")
            cur = cnn.cursor()
            if not cur:raise (NameError, "Can not connect database")
            else:return cur, cnn

    def execQuery(self, sql):
                """执行查询语句"""
                try:
                    cur, cnn = self.dbConnect()
                    if cur:
                        cur.execute(sql)
                        resList = cur.fetchall()
                        # 查询完毕后关闭连接
                        cnn.close()
                        return resList
                except Exception, e:
                    print u'sql语句==', sql
                    print u'执行查询语句的时候报错了，e=', e
        # ++++++++++++++++++++++++++++++++++++++++++++++++

    def predictData(self,dict_data):
        """情感分类"""
        """传进来一个字典类型数据，对字典的key，value进行分析，得到key对应的value属于什么情感分类"""
        for k,v in dict_data.items():
            print 'text={},positive_ratio={}'.format(k,v) #pos_ratio

    def readFile(self,path):
        """"读取文件"""
        returnPath=self.convertEncoding(path=path,target_encoding='utf-8')
        if returnPath is not None:
            path=returnPath
        data = codecs.open(path, 'rb', encoding='utf-8', errors='ignore').readlines()
        return data

    def checkEncoding(self,str):
        return chardet.detect(str)['encoding']

    def convertEncoding(self,path,target_encoding):
        """文件编码转换"""
        """如果文件的编码跟tartgetEncoding不吻合,则将文本转换为目标编码,然后存储到原目录,最后重新以targetEncoding读取"""
        dd = codecs.open(path, 'r').readlines()
        encoding_Truth = self.checkEncoding(dd[0])
        print u'encoding_Truth==',encoding_Truth
        if encoding_Truth != target_encoding:
            content=(n.decode(encoding_Truth, 'ignore') for n in dd)
            #content=dd.decode(encoding_Truth, 'ignore')
            for n in content:
                codecs.open(self.changeTestPath,'a+',encoding=target_encoding).write(n+"\n")
            return self.changeTestPath
        else:return path

    def filter_remove(self,txt):
        """
        remove掉TXT中的某些文本
        :param txt:
        :return:
        """
        returnData=[re.sub(k, v, txt) for k, v in self.dateTimeTextDict.items() if k in txt]
        if len(returnData)>0:return returnData[0]
        else:return txt

    def handleDatas(self,process_=None,manageList_=None):
        """传入子进程ID，进程全局变量list"""
        # 根据股票code获取数据库里的数据
        print 'manageList_==',manageList_
        for code in self.allCodeData:
            code=code[:6]
            print u'当前code==',code
            lock.acquire()
            if code in manageList_ or code in self.someCodeData:
                lock.release()
                continue
            print u'过滤后的code==',code
            manageList_.append(code)
            print u'manageList==', manageList_
            sql="SELECT [code],CONVERT(nvarchar(1000),[author_Name]),CONVERT(nvarchar(max),[title]),CONVERT(nvarchar(max)" \
                ",[datas]),CONVERT(nvarchar(200),[upTime]) FROM [LiangJingJun].[dbo].[SnowStockTalks] where code='{}'".format(code)
            print 'sql==',sql
            lock.release()
            returnData=self.execQuery(sql=sql.encode('utf-8'))
            if not returnData:continue
            factorDF=pd.DataFrame(data=returnData,columns=['code','author','title','datas','upTime'])
            factorDF['upTime'] = map(self.filter_remove, factorDF['upTime'].values)
            Sentimentdf=self.checkDataNLP(factorDF=factorDF,code_=code)
            arrayList=Sentimentdf.values
            #print 'Sentimentdf==',arrayList
            #print 'datasSentimentdf==', datasSentimentdf
            #cur, cnn = self.dbConnect()
            #self.multi_insert_table(arrayItems=arrayList, conn=cnn, cursor=cur, table='Factor_SnowStockTalks',chunk_size=100)


    def multi_insert_table(self,arrayItems, conn, cursor, table='Factor_SnowStockTalks', chunk_size=100):
        """
        手动优化<一次多行写入数据库指定TABLE>
        """
        all_rows = map(lambda row:"('" + str(row[2]) +"','" + str(row[0]) + "','" + str(row[1]) + "','" + str(row[4]) +
                                  "','"+ str(row[5]) +"','"+ str(row[6]) +"','"+ str(row[7]) +"','"+ str(row[8]) +"','"+
                                  str(row[9]) +"','"+ str(row[10]) + "','"+ str(row[11]) +"','"+ str(row[3]) + "')",[row for row in arrayItems])
        #print u'all_rows==',all_rows
        sql = 'insert into ' + table +'([code],[titleMarketing],[datasMarketing],[title_Rolling5_mean],[title_Rolling10_mean],' \
                                      '[title_Rolling20_mean],[title_Rolling30_mean],[datas_Rolling5_mean],[datas_Rolling10_mean],' \
                                      '[datas_Rolling20_mean],[datas_Rolling30_mean],[dateUpTime]) values '
        lenrow=len(all_rows)
        n,k = lenrow // chunk_size,lenrow % chunk_size
        for i in range(n):
            multi_rows = ', '.join(all_rows[i * chunk_size:(i + 1) * chunk_size])
            cursor.execute(sql + multi_rows)
            #print u'i===>sql-multi_rows==', sql + multi_rows
        if k:
            multi_rows = ', '.join(all_rows[-k:])
            cursor.execute(sql + multi_rows)
            #print u'k===>sql-multi_rows==', sql + multi_rows
        conn.commit()
        conn.close()
        return None

    def trainSentimentCorpus(self, negPath, posPath, target_encoding):
        """训练Sentiment语料库"""
        self.convertEncoding(negPath, target_encoding)
        self.convertEncoding(posPath, target_encoding)
        # pos_docs = codecs.open(posPath, 'r', 'utf-8').readlines()
        sentiment.train(neg_file=negPath, pos_file=posPath)
        path_name = 'sentiment_Jxl_line'
        print u'数据训练完毕，即将保存{}.marshal文件'.format(path_name)
        sentiment.save('{}.marshal'.format(path_name))
        print u'保存完毕!'

def initLock(lock_):
    global lock
    lock=lock_

if __name__ == '__main__':
    try:
        manage = multiprocessing.Manager()
        process_N = 5  # N个子进程
        lock=multiprocessing.Semaphore(process_N*2) #(打旗语)用来控制对共享资源的访问数量，例如池的最大连接数。
        manageList = manage.list()
        pool = multiprocessing.Pool(processes=process_N,initializer=initLock,initargs=(lock,))
        #partial_send_request(func,lock=lock)
        # 创建一个进程池pool，并设定进程的数量为3，arange(4)会相继产生四个对象[0, 1, 2, 3,4]，四个对象被提交到pool中，\
        # 因pool指定进程数为3，所以0、1、2会直接送到进程中执行，当其中一个执行完后才空出一个进程处理对象3或4
        for n in np.arange(process_N):
            pool.apply_async(func, args=(n, manageList,))
    except Exception,e:print e
    print 'Waiting for all child Process done...'
    pool.close()
    pool.join()
    print 'All child Process is done'