# coding = utf-8
# 功能：抓取基金新增历史数据，更新到全量基金数据

import pandas as pd
import os
import datetime
from base import constdef
from stockutil.dataloadutil.loadfundutil import load_fund_code
from datacrawl.crawlfundwork import CrawlFundData
from base.workutil import caltimediff
from pandas.io.common import EmptyDataError

out_dir = constdef.fund_data_dir
s_date = "2001-12-01"
e_date = "2099-12-31"
min_code_value = 0


def save_to_disk(t_fileName, pd_data):
    try_times = 0
    iResult = 0

    while try_times < 5:
        try:
            pd_data.to_csv(t_fileName, columns=pd_data.columns.values.tolist(), index=True)
            iResult = 0
            break
        except OSError:
            try_times = try_times + 1
            print("Error to save to file,try times:%d (%s)" % (try_times, t_fileName))
            iResult = 1

    return iResult


def crawl_new_data(code_str,crawlHandle,work_start_time):
    target_file_name = out_dir + code_str + ".csv"

    load_data = None
    if os.path.exists(target_file_name):
        try:
            load_data = pd.read_csv(target_file_name, dtype=object, parse_dates=False, index_col=0)
            load_data.sort_index(inplace=True)
        except EmptyDataError:
            load_data = None

    crawlHandle.init_work(code_str, s_date, e_date)
    fund_data = crawlHandle.do_crawl_new_data()

    currtime = datetime.datetime.now()
    hour, min, sec = caltimediff(work_start_time, currtime)
    if (fund_data is None):
        print("done for crawl %s,Net error happend,No data! spend times: %d hours,%d mins,%dsecs" % (code_str,hour, min, sec))
        return

    if load_data is None:
        save_to_disk(target_file_name, fund_data)
        currtime = datetime.datetime.now()
        hour, min, sec = caltimediff(work_start_time, currtime)
        print("done for crawl %s,total count:%d,crawl new count:%d, spend times: %d hours,%d mins,%dsecs" % (code_str, len(fund_data), len(fund_data),hour, min, sec))
    else:
        fund_data.set_index('date', inplace=True)
        load_data.set_index('date', inplace=True)
        load_data = load_data.append(fund_data)

        load_data.index.name = ''
        load_data.insert(0, 'date', load_data.index)

        load_data.drop_duplicates('date', 'first', inplace=True)

        load_data.sort_index(inplace=True)
        save_to_disk(target_file_name, load_data)

        currtime = datetime.datetime.now()
        hour, min, sec = caltimediff(work_start_time, currtime)
        print("done for crawl %s,total count:%d,crawl new count:%d, spend times: %d hours,%d mins,%dsecs" % (code_str, len(load_data),len(fund_data),hour, min, sec))
        
        
if __name__ == '__main__':
    print("Begin to crawl all Fund new data!")
    starttime = datetime.datetime.now()
    
    code_list = load_fund_code(includedsellclosed=False)
    print("Finish to load fund code, fund count:%d" % (len(code_list)))
    
    crawl_work = CrawlFundData()
    
    for code_str in code_list:
        if not (code_str.isdigit()):
            print("Fund code %s is not Number" % (code_str))
            continue
        if int(code_str) < min_code_value:
            continue
        crawl_new_data(code_str, crawl_work, starttime)
        
    currtime = datetime.datetime.now()
    hour, min, sec = caltimediff(starttime, currtime)
    print("Done from Fund new data crawl work! Total spand times: %d hours,%d mins,%dsecs ." % (hour, min, sec))