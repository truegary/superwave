# coding = utf-8


#功能说明：计算时间差
def caltimediff(time_start,time_end):
    sec_value = (time_end - time_start).seconds
    hour = int(sec_value/3600)
    min = int((sec_value - hour*3600)/60)
    sec = sec_value - hour*3600 - min*60
    return hour, min, sec