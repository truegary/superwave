# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib.request
import traceback
import time
import json
import pandas as pd

base_url = "http://summary.jrj.com.cn"
url_group_index = "/hybk/index.shtml?q=cn|bk|17&n=hqa&c=l&o=pl,d&p=1050"
subgroup_url_mode = 'http://q.jrjimg.cn/?q=cn|s|bk%s&c=m&n=hqa&o=pl,d&p=%d50'
crawl_time_out = 5
max_retry_times = 3
crawl_sleep_time = 5
page_coding = "GB2312"
group_industry_flag = ["行业板块", "popmenu1"]
group_concept_flag = ["概念板块", "popmenu_cb"]
group_ZONE_flag = ["地域板块", "popmenu_rb"]
group_SFC_flag = ["证监会行业", "popmenu_fb"]
group_global_flag = ["全球行业", "popmenu_gb"]
MAX_STOCK_COUNT_OFTYPE = 9
OUT_FILE = "/workdata/common_data/stock_in_type.csv"


def doCrawlGroupIndex(page_url):
    page_data = None
    try_times = 0

    while (try_times < max_retry_times):
        try:
            html = urllib.request.urlopen(page_url, timeout=crawl_time_out).read()
            html = html.decode(page_coding)
            page_data = BeautifulSoup(html, 'html.parser')
            break
        except:
            print("http net error happend! retry times %d" % (try_times))
            print('traceback.print_exc():%s' % (str(traceback.print_exc())))
            print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
            page_data = None
            try_times = try_times + 1
            time.sleep(crawl_sleep_time)

    if page_data is None:
        return None

    return page_data


def doCrawlGroupdata(page_url):
    page_data = None
    try_times = 0

    while (try_times < max_retry_times):
        try:
            html = urllib.request.urlopen(page_url, timeout=crawl_time_out).read()
            html = html.decode(page_coding)
            page_data = json.loads(html)
            break
        except:
            print("http net error happend! retry times %d" % (try_times))
            print('traceback.print_exc():%s' % (str(traceback.print_exc())))
            print('traceback.format_exc():\n %s' % str(traceback.format_exc()))
            page_data = None
            try_times = try_times + 1
            time.sleep(crawl_sleep_time)

    if page_data is None:
        return None

    return page_data


def get_group_info(in_data, data_flag, flag_type):

    data_group_name = data_flag[0]
    data_group_flag = data_flag[1]

    content = in_data.select('div [id="%s"]' % (data_group_flag))
    if len(content) <= 0:
        return None

    if flag_type == 0:
        data_item = content[0].select('div[class="col"] > a')
    else:
        data_item = content[0].select('div[class="col"] > div > a')

    if len(data_item) <= 1:
        return None

    main_group_info = []
    for i in range(1, len(data_item)):
        print("%s %s %s" % (data_group_name, data_item[i].text, base_url+data_item[i].attrs['href']))
        main_group_info.append([data_group_name, data_item[i].text, base_url+data_item[i].attrs['href']])

    return main_group_info


def load_group_detail_data(group_detail_url, main_type, sub_type):
    subgroup_page = doCrawlGroupIndex(group_detail_url)

    if subgroup_page is None:
        return None

    subgroup_page = str(subgroup_page)

    start_pos = subgroup_page.find("HqData:[")
    if start_pos < 0:
        return None

    subgroup_page = subgroup_page[start_pos + 8:-4]

    target_str = subgroup_page.find("],")
    if target_str < 0:
        return None

    groupdatas = subgroup_page.split("],")
    if len(groupdatas) == 0:
        return None

    group_stocks = []
    for data_str in groupdatas:
        data_items = data_str.split(",")
        group_stocks.append([main_type, sub_type, data_items[1][1:-1], data_items[2][1:-1]])

    return group_stocks


def load_group_detail(subgroup_params):
    subgroup_url = subgroup_params[2]
    start_pos = subgroup_url.find("/400")
    if start_pos < 0:
        return None
    subgroup_url_flag = subgroup_url[start_pos+1:-6]

    group_detail_data = []
    for i in range(MAX_STOCK_COUNT_OFTYPE):
        sub_url = subgroup_url_mode % (subgroup_url_flag, (i+1)*10)
        group_stocks = load_group_detail_data(sub_url, subgroup_params[0], subgroup_params[1])
        if group_stocks is None:
            break
        else:
            group_detail_data.extend(group_stocks)

    return group_detail_data


if __name__ == '__main__':
    page_data = doCrawlGroupIndex(base_url+url_group_index)
    if page_data is None:
        print("No data is in the page. Done!")
        exit(0)

    all_stock_in_group_data = []
    sub_groups = get_group_info(page_data, group_industry_flag, 0)
    for data_item in sub_groups:
        print("load stock data in %s %s" % (data_item[0], data_item[1]))
        stock_in_group_data = load_group_detail(data_item)
        if not(stock_in_group_data is None):
            all_stock_in_group_data.extend(stock_in_group_data)

    sub_groups = get_group_info(page_data, group_concept_flag, 0)
    for data_item in sub_groups:
        print("load stock data in %s %s" % (data_item[0], data_item[1]))
        stock_in_group_data = load_group_detail(data_item)
        if not(stock_in_group_data is None):
            all_stock_in_group_data.extend(stock_in_group_data)

    sub_groups = get_group_info(page_data, group_ZONE_flag, 1)
    for data_item in sub_groups:
        print("load stock data in %s %s" % (data_item[0], data_item[1]))
        stock_in_group_data = load_group_detail(data_item)
        if not(stock_in_group_data is None):
            all_stock_in_group_data.extend(stock_in_group_data)

    sub_groups = get_group_info(page_data, group_SFC_flag, 1)
    for data_item in sub_groups:
        print("load stock data in %s %s" % (data_item[0], data_item[1]))
        stock_in_group_data = load_group_detail(data_item)
        if not(stock_in_group_data is None):
            all_stock_in_group_data.extend(stock_in_group_data)

    sub_groups = get_group_info(page_data, group_global_flag, 1)
    for data_item in sub_groups:
        print("load stock data in %s %s" % (data_item[0], data_item[1]))
        stock_in_group_data = load_group_detail(data_item)
        if not(stock_in_group_data is None):
            all_stock_in_group_data.extend(stock_in_group_data)

    pd_stock_type = pd.DataFrame(data=all_stock_in_group_data, columns=['Main_type', 'sub_type', 'code', 'stockname'])
    pd_stock_type.to_csv(OUT_FILE, index=None)

    print("Done!")
