# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib.request
import traceback
import time

url_group_index = "http://summary.jrj.com.cn/hybk/index.shtml?q=cn|bk|17&n=hqa&c=l&o=pl,d&p=1050"
crawl_time_out = 5
max_retry_times = 3
crawl_sleep_time = 5
page_coding = "GB2312"
group_industry_flag = "popmenu1"