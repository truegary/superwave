# coding = utf-8

# 生成以年K线计算，前一年下降一定比例后，第二年开盘买入的获利数据情况

from stockutil.dataloadutil.loadfundutil import load_stock_code
from analysis_work.strategyutil.jump_chance_after_long_down import year_jump_chance_checker
import pandas as pd


s_year = 2000
e_year = 2017
d_rate = 0
next_u_rate = 0.1
OUT_FILE = "/Volumes/macdisk2/garywork/stock/analysis_data/year_jump_analysis_data.csv"


def gendataBystockcode(stockcode,from_year,end_year,down_rate,next_up_rate):
    start_year = from_year
    rtns = []
    while start_year <=  end_year:
        result_data = year_jump_chance_checker(stockcode, start_year, down_rate, e_earn_rate=next_up_rate)
        if (not (result_data is None)) and (len(result_data) >= 5) :
            result_data = [start_year, stockcode, result_data[0], result_data[1], result_data[2], result_data[3], result_data[4]]
            print(result_data)
            rtns.append(result_data)
        else:
            print("No data with stock %s in year %d" % (stockcode, start_year))
        start_year = start_year + 1

    return rtns


if __name__ == '__main__':
    all_result_data = []

    stock_code_list = load_stock_code()
    for s_code in stock_code_list:
        check_data = gendataBystockcode(s_code,s_year,e_year,d_rate,next_u_rate)
        all_result_data.extend(check_data)

    pd_data = pd.DataFrame(columns=["year","stockcode","sample_status","islowOpen","down_rate","check_result","real_rate"],data=all_result_data)
    pd_data.to_csv(OUT_FILE, index=None)