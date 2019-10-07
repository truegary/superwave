# coding = utf-8
import requests
import traceback
import time
from base import constdef
import json
import pandas as pd
from stockutil.dataloadutil.loadfundutil import load_stock_data
from datetime import datetime


class CrawlStockData(object):
    def __init__(self):
        self.__curr_page__ = 0
        self.__base_url_one__ = "http://quotes.money.163.com/service/chddata.html?code=%d%s&start=%s&end=%s&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;"
        self.__max_retry_times__ = 3
        self.__sleep_times__ = 3
        
    def setSleepTime(self,stime_sec):
        self.__sleep_times__ = stime_sec

    def setMaxRetryTimes(self,retry_times):
        self.__max_retry_times__ = retry_times
        
    def do_crawl(self, scode, start_date=None, end_date=None):
        s_date = "1995-01-01"
        e_date = "2099-12-31"
        if not (start_date is None):
            s_date = start_date
        if not (end_date is None):
            e_date = end_date
        
        stock_data_url = (self.__base_url_one__) % (0, scode, s_date, e_date)
        if scode[0:2] != "60":
            stock_data_url = (self.__base_url_one__) % (1, scode, s_date, e_date)
            
       
        
        try_times = 0
        while (try_times < self.__max_retry_times__):
            try:
                self.__do_crawl_data_save_to_file__(scode, stock_data_url)
                break
            except:
                print("http net error happend!stock code:%s,retry times %d" % (scode,try_times))
                print('traceback.print_exc():%s' % (str(traceback.print_exc())))
                print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
                print("stock %s crawl error info end!==============" % (scode))
                try_times = try_times + 1
                time.sleep(self.__sleep_times__)
        
        if try_times >= self.__max_retry_times__:
            return 1
        else:
            out_file_name = constdef.data_path + scode + ".csv"
            
            load_data = pd.read_csv(out_file_name, dtype=object, parse_dates=False, encoding = 'ANSI')
            load_data['股票代码'] = scode
            load_data = pd.DataFrame(load_data, columns=['日期','股票代码','收盘价','最高价','最低价','开盘价','前收盘','涨跌额','涨跌幅','成交量'])
            load_data.to_csv(out_file_name,header=True,index=None)
            return 0

    def __do_crawl_data_save_to_file__(self, scode, page_url):
        out_file_name = constdef.data_path + scode + ".csv"
        r = requests.get(page_url, timeout=5) 
        with open(out_file_name, "wb") as code:
            code.write(r.content)
            
    def __do_crawl_data__(self, page_url):
        r = requests.get(page_url, timeout=5)
        return r.content
    
    def __do_load_data__(self, scode):
        b_url = "http://q.stock.sohu.com/hisHq?code=cn_%s&stat=1&order=D&period=d&callback=historySearchHandler&rt=jsonp"
        crawl_url = b_url % (scode)
        
        try_times = 0
        sdata = None
        while (try_times < self.__max_retry_times__):
            try:
                sdata = self.__do_crawl_data__(crawl_url)
                return 0, sdata
            except:
                print("http net error happend!stock code:%s,retry times %d" % (scode,try_times))
                print('traceback.print_exc():%s' % (str(traceback.print_exc())))
                print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
                try_times = try_times + 1
                time.sleep(self.__sleep_times__)
                      
        return 1, None
    
    def do_crawl_new_data(self, scode):
        crawl_result, sdata = self.__do_load_data__(scode)
        if crawl_result != 0:
            return 1
        
        data_str = str(sdata)
        
        start_index = data_str.find("[[")
        end_index = data_str.find("]]")
        
        if (start_index < 0) or (end_index < 0):
            return 2
        
        data_str = sdata[start_index-2:end_index]
        
        hjson = json.loads(data_str)
        data_count = len(hjson)
        data_index = 0
        page_data = []
        while (data_index < data_count):
            change_rate = float(str(hjson[data_index][4])[:-1])
            page_data.append([hjson[data_index][0], float(hjson[data_index][1]), float(hjson[data_index][2]), float(hjson[data_index][3]), change_rate, float(hjson[data_index][5]), float(hjson[data_index][6]), int(hjson[data_index][7]) * 100 ])
            data_index = data_index + 1
            
        pd_page_data = pd.DataFrame(columns=['日期','开盘价','收盘价','涨跌额','涨跌幅','最低价','最高价','成交量'], data=page_data)
        pd_page_data['前收盘'] = pd_page_data['收盘价'].shift(-1)
        pd_page_data = pd_page_data.drop([len(pd_page_data) - 1])
        pd_page_data['股票代码'] = scode
        
        pd_page_data = pd_page_data[['日期','股票代码','收盘价','最高价','最低价','开盘价','前收盘','涨跌额','涨跌幅','成交量']]
        
        if len(pd_page_data) == 0:
            return 0
        
        stock_date = pd_page_data['日期'].values.tolist()
        pd_page_data.index = [datetime.strptime(d, '%Y-%m-%d').date() for d in stock_date]
        pd_page_data.sort_index(inplace=True)
        
        out_file_name = constdef.data_path + scode + ".csv"
        old_data = load_stock_data(scode,keep_no_trade=1)
        
        if (old_data is None) or (len(old_data) == 0) :
            pd_page_data.sort_index(inplace=True, ascending=False)
            pd_page_data.to_csv(out_file_name, columns=pd_page_data.columns.values.tolist(), index=None)
            return 0
        
        old_data = old_data.append(pd_page_data)
        old_data.drop_duplicates('日期', 'first', inplace=True)
        old_data.sort_index(inplace=True, ascending=False)
        old_data.to_csv(out_file_name, columns=old_data.columns.values.tolist(), index=None)
        return 0
