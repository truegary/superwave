# coding = utf-8

import os
import pandas as pd

his_data_dir = "/Volumes/macdisk2/garywork/stock/hisstockdata"
his_data_file_head = "sdata"
out_data_file = "/Volumes/macdisk2/garywork/stock/stockdata/month/allmonth.csv"


def gethisdatafiles(basedir, filehead):
    file_names = []
    for root, dirs, files in os.walk(basedir):
        for filename in files:
            if filename.startswith(filehead):
                file_names.append(basedir+"/"+filename)
    return file_names


def loaddatabyfile(filename):
    src_data = pd.read_excel(filename, sheet_name=1, header=1)
    src_data.drop(src_data.tail(2).index, inplace=True)
    col_names = src_data.columns.tolist()
    col_names[2] = '名称'
    src_data.columns = col_names
    src_data['时间'] = src_data['时间'].astype(str)
    src_data['时间'] = src_data['时间'].str[0:10]
    src_data['代码'] = src_data['代码'].str[0:6]

    src_data['成交额'] = src_data['成交额'].map(lambda x: ('%.2f') % x)

    for i in range(3, 9):
        src_data.iloc[:, i] = (src_data.iloc[:, i]).map(lambda x: ('%.2f') % x)

    return src_data


if __name__ == '__main__':
    all_files = gethisdatafiles(his_data_dir, his_data_file_head)

    all_sub_datas = []

    print("start...")
    for file_item in all_files:
        print("load file:%s" % file_item)
        all_sub_datas.append(loaddatabyfile(file_item))

    all_pddatas = pd.concat(all_sub_datas)

    all_pddatas.to_csv(out_data_file, columns=all_pddatas.columns.values.tolist(), index=None)
    print("done!")

