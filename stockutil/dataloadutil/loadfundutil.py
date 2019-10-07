# coding = utf-8

import pandas as pd
import os
from base import constdef
from datetime import datetime


def load_fund_code(includedsellclosed=True):
    code_data = pd.read_csv(constdef.code_file, dtype=object)
    if (code_data is None) or (len(code_data) == 0):
        return None
    
    if not includedsellclosed:
        code_data = code_data[code_data['封闭型'] == '0']
    
    code_list = code_data['编码'].tolist()
    return code_list


def load_fund_data(fund_code, first_date=None, last_date=None):
    if last_date is None:
        data_last_date = constdef.e_date
    else:
        data_last_date = last_date

    if first_date is None:
        data_first_date = constdef.s_date
    else:
        data_first_date = first_date

    fullname = constdef.fund_data_dir + fund_code + ".csv"

    if not os.path.exists(fullname):
        return None

    load_data = pd.read_csv(fullname, parse_dates=True, index_col=0)
    load_data.sort_index(inplace=True)
   
    return load_data[(load_data['date'] >= data_first_date) & (load_data['date'] <= data_last_date)]


# load_stock_code 获取股票代码
# 参数说明：0 - 所有代码, 1 - 沪市股票, 2 - 深市股票, 3 - 创业板股票, 4 - 非股票 , 99 - 所有股票
def load_stock_code(code_type=99):
    code_data = pd.read_csv(constdef.stock_code_file, dtype=object)
    if (code_data is None) or (len(code_data) == 0):
        return None
    
    code_data['code_head'] = code_data['stock_code'].str[0:2]
    
    if code_type == 1:
        code_data = code_data[(code_data['code_head'] == '60')]
        return code_data['stock_code'].tolist()
    
    if code_type == 2:
        code_data = code_data[(code_data['code_head'] == '00')]
        return code_data['stock_code'].tolist()
    
    if code_type == 3:
        code_data = code_data[(code_data['code_head'] == '30')]
        return code_data['stock_code'].tolist()
    
    if code_type == 4:
        code_data = code_data[(code_data['code_head'] != '60') & (code_data['code_head'] != '00') & (code_data['code_head'] != '30')]
        return code_data['stock_code'].tolist()
    
    if code_type == 99:
        code_data = code_data[(code_data['code_head'] == '60') | (code_data['code_head'] == '00') | (code_data['code_head'] == '30')]
        return code_data['stock_code'].tolist()
    
    return None


def load_stock_data(stock_code, first_date=None, last_date=None,keep_no_trade=0):
    if last_date is None:
        data_last_date = constdef.e_date
    else:
        data_last_date = last_date

    if first_date is None:
        data_first_date = constdef.s_date
    else:
        data_first_date = first_date
        
    fullname = constdef.data_path + stock_code + ".csv"
    
    if not os.path.exists(fullname):
        return None

    load_data = pd.read_csv(fullname, dtype=object, parse_dates=False)
    stock_date = load_data['日期'].values.tolist()
    load_data.index = [datetime.strptime(d, '%Y-%m-%d').date() for d in stock_date]
    load_data.sort_index(inplace=True)
    
    load_data['收盘价'] = load_data['收盘价'].astype('float64')
    load_data['最高价'] = load_data['最高价'].astype('float64')
    load_data['最低价'] = load_data['最低价'].astype('float64')
    load_data['开盘价'] = load_data['开盘价'].astype('float64')
    load_data['前收盘'] = load_data['前收盘'].astype('float64')
    
    
    if keep_no_trade == 0:
        load_data = load_data[(load_data['收盘价'] > 0.0)]
    
    
    return load_data[(load_data['日期'] >= data_first_date) & (load_data['日期'] <= data_last_date)]


def format_code(in_value):
    if in_value is None:
        return None
    
    value_str = str(in_value)
    if len(value_str) == 0:
        return None
    
    return value_str.zfill(6)