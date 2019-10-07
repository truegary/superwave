# coding = utf-8

from bs4 import BeautifulSoup
import urllib.request
from datetime import datetime
import pandas as pd
import time
import traceback

class CrawlFundData(object):
    def __init__(self):
        self.__curr_page__ = 0
        self.__fund_code__ = ""
        self.__base_url_one__ = "http://quotes.money.163.com/fund/jzzs_%s_%d.html?start=%s&end=%s&sort=TDATE&order=desc"
        self.__data_flag__ = "div[class='fn_fund_tb_content'] > table > tbody > tr"
        self.__data_item_flag__ = "td"
        self.__max_page_count__ = 200
        self.__start_date__ = ""
        self.__end_date__ = ""
        self.__page_code__ = "utf-8"
        self.__max_retry_times__ = 3
        self.__sleep_times__ = 3

    def setSleepTime(self,stime_sec):
        self.__sleep_times__ = stime_sec

    def setMaxRetryTimes(self,retry_times):
        self.__max_retry_times__ = retry_times

    def setMaxPageCount(self, pcount):
        self.__max_page_count__ = pcount

    def setPageCoding(self,pcode):
        self.__page_code__ = pcode

    def init_work(self, fcode, s_date, e_date):
        self.__curr_page__ = 0
        self.__fund_code__ = fcode
        self.__start_date__ = s_date
        self.__end_date__ = e_date

    def do_crawl(self):
        try_times = 0
        while (try_times < self.__max_retry_times__):
            try:
                fund_data = self.__do_crawl__(self.__base_url_one__)
                return fund_data
            except:
                print("http net error happend!fund code:%s,retry times %d" % (self.__fund_code__,try_times))
                print('traceback.print_exc():%s' % (str(traceback.print_exc())))
                print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
                try_times = try_times + 1
                time.sleep(self.__sleep_times__)
        return None

    def do_crawl_new_data(self):
        try_times = 0
        while (try_times < self.__max_retry_times__):
            try:
                fund_data = self.__do_crawl_new_data__(self.__base_url_one__)
                return fund_data
            except:
                print("http net error happend!fund code:%s,retry times %d" % (self.__fund_code__,try_times))
                print('traceback.print_exc():%s' % (str(traceback.print_exc())))
                print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
                try_times = try_times + 1
                time.sleep(self.__sleep_times__)
        return None

    def __do_crawl_new_data__(self,b_url):
        result_data = []
        result_data_index = []

        page_Url = b_url % (self.__fund_code__, 0, self.__start_date__, self.__end_date__)

        if self.__do_crawl_page__(page_Url, result_data, result_data_index) != 0:
            result_data = []
            result_data_index = []

        col_name = ['date', 'nav', 'acc_nav', 'rate']
        fund_data = pd.DataFrame(columns=col_name, data=result_data, index=result_data_index)

        return fund_data

    def __do_crawl__(self, b_url):
        result_data = []
        result_data_index = []
        for page_no in range(self.__max_page_count__):
            page_Url = b_url % (self.__fund_code__,page_no,self.__start_date__,self.__end_date__)
            if self.__do_crawl_page__(page_Url,result_data,result_data_index) != 0:
                break

        col_name = ['date', 'nav', 'acc_nav', 'rate']
        fund_data = pd.DataFrame(columns=col_name, data=result_data, index=result_data_index)
        return fund_data

    def __do_crawl_page__(self, page_url, all_data, index_data):
        html = urllib.request.urlopen(page_url,timeout=3).read()
        html = html.decode(self.__page_code__)
        soup = BeautifulSoup(html, 'lxml')
        content = soup.select(self.__data_flag__)

        if (content is None) or (len(content) == 0):
            return 1

        for page_item in content:
            item_value = page_item.select(self.__data_item_flag__)
            if len(item_value) >= 4:
                item_record = [item_value[0].text, item_value[1].text, item_value[2].text, item_value[3].text[:-1]]
                date_index_item = datetime.strptime(item_value[0].text, '%Y-%m-%d').date()
                all_data.append(item_record)
                index_data.append(date_index_item)

        return 0
