# coding = utf-8
# 功能：抓取股票最新历史数据
from base.workutil import caltimediff
from datacrawl.crawlstockkdata import CrawlStockData
from stockutil.dataloadutil.loadfundutil import load_stock_code
import datetime


def do_work(starttime):
    stock_code_list = load_stock_code()
    
    crawl_handle = CrawlStockData()
    done_count = 0
    
    for s_code in stock_code_list:
        crawl_result = crawl_handle.do_crawl_new_data(s_code)
        currtime = datetime.datetime.now()
        hour, min, sec = caltimediff(starttime, currtime)
        if crawl_result == 0:
            print("Crawl stock data %s Done! spend times: %d hours,%d mins,%dsecs" % (s_code, hour, min, sec))
            done_count = done_count + 1
        else:
            print("Failed to crawl stock data %s Done! spend times: %d hours,%d mins,%dsecs" % (s_code, hour, min, sec))
    
    return done_count


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    print("Begin to crawl all stock new data!")
    
    crawl_count = do_work(starttime)
    
    currtime = datetime.datetime.now()
    hour, min, sec = caltimediff(starttime, currtime)
    print("Done for stock new data crawl work! Total spend times: %d hours,%d mins,%dsecs,stock count:%d" % (hour, min, sec,crawl_count))
    