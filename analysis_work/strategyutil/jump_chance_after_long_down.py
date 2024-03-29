# coding = utf-8

# 策略思路：在较长时间（如一年）下跌后，会引发一定的反弹，即在下一个同一时间段会有一定的涨幅


from stockutil.dataloadutil.loadfundutil import load_stock_data


def year_jump_chance_checker(stockcode, year, down_rate=0.4, min_trade_days=60, e_earn_rate=None):
    s_data = load_stock_data(stockcode)
    data_in_year = s_data[s_data['日期'].str[0:4] == str(year)]
    if len(data_in_year) < min_trade_days:
        return None
    data_in_preyear = s_data[s_data['日期'].str[0:4] == str(year - 1)]
    if len(data_in_preyear) < min_trade_days:
        return None
    data_in_pre2year = s_data[s_data['日期'].str[0:4] == str(year - 2)]
    if len(data_in_pre2year) < 1:
        return None

    data_in_nextyear = s_data[s_data['日期'].str[0:4] == str(year + 1)]
    if len(data_in_nextyear) < 1:
        return None

    close_year_price = data_in_year.tail(1)['收盘价'].values[0]
    open_year_price = data_in_year.head(1)['开盘价'].values[0]
    block_low_year = data_in_year.tail(1)['收盘价'].values[0]

    open_next_price = data_in_nextyear.head(1)['开盘价'].values[0]
    if block_low_year > open_year_price:
        block_low_year = open_year_price

    stock_down_rate = (close_year_price - open_year_price)/open_year_price

    isTargetStock = True


    if stock_down_rate > (0 - down_rate):
        isTargetStock = False

    islowopen = 0
    if block_low_year > open_next_price:
        islowopen = 1
        isTargetStock = False

    val_isTargetStock = 0
    if not isTargetStock:
        val_isTargetStock = 1

    if e_earn_rate is None:
        return [val_isTargetStock, islowopen, stock_down_rate]

    if len(data_in_nextyear) < min_trade_days:
        return [isTargetStock, -1, -1]

    next_open_price = data_in_nextyear.head(1)['开盘价'].values[0]
    next_high_price = max(data_in_nextyear['最高价'])

    next_earn = (next_high_price - next_open_price)/next_open_price

    if next_earn >= e_earn_rate:
        return [val_isTargetStock, islowopen, stock_down_rate, 0, next_earn]
    else:
        return [val_isTargetStock, islowopen, stock_down_rate, 1, next_earn]


if __name__ == '__main__':
    rtn_result = year_jump_chance_checker("600036", 2017, 0.4, e_earn_rate=0.1)
    print(rtn_result)

