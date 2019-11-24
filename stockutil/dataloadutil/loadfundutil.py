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


def load_multi_stock_datas(stock_code_list, first_date=None, last_date=None, keep_no_trade=0):
    m_close = None
    for scode in stock_code_list:
        m_data = load_stock_data(scode, first_date, last_date, keep_no_trade)
        if (m_data is None) or (len(m_data) == 0):
            continue
        if m_close is None:
            m_close = pd.DataFrame(data=m_data['收盘价'])
            m_close.columns = [scode]
        else:
            m_close.insert(0, scode, m_data['收盘价'])
    return m_close


def format_code(in_value):
    if in_value is None:
        return None
    
    value_str = str(in_value)
    if len(value_str) == 0:
        return None
    
    return value_str.zfill(6)


def load_stockcode_in_catigery(level1_name, level2_name=None):
    code_data = pd.read_csv(constdef.stock_code_in_categery, dtype=object)
    code_data.columns = ['Main_type', 'sub_type', 'code', 'stockname']

    code_data = code_data[code_data['Main_type'] == level1_name].copy()
    if (level2_name is None) or (level2_name == ''):
        return code_data['code'].values.tolist()

    code_data = code_data[code_data['sub_type'] == level2_name].copy()
    return code_data['code'].values.tolist()

def load_stock_month_data():
    stock_data = pd.read_csv(constdef.stock_month_file,
                             dtype={"时间":str, "代码":str, "名称":str, "交易状态":str, "是否涨停":str, "是否跌停":str})

    stock_data['成交额'] = stock_data['成交额'].map(lambda x: ('%.2f') % x)

    for i in range(3, 9):
        stock_data.iloc[:, i] = (stock_data.iloc[:, i]).map(lambda x: ('%.2f') % x)

    stock_data['成交量'] = stock_data['成交量'].fillna(0).astype(int)
    return stock_data



