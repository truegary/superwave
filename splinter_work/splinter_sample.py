# -*- coding: utf-8 -*-

from splinter import Browser
from time import sleep


url = "https://kyfw.12306.cn/otn/leftTicket/init"
login_url = "https://kyfw.12306.cn/otn/view/index.html"


def do_login(browser):
    browser.visit(login_url)
    sleep(1)
    browser.find_by_id("J-userName").fill("")
    browser.find_by_id("J-password").fill("")

    login_btn = browser.find_by_text("立即登录")
    login_btn.click()


def do_buy_ticket(browser):
    browser.visit(url)
    browser.cookies.add({"_jc_save_fromDate": "2019-10-08"})
    browser.cookies.add({"_jc_save_fromStation": "%u6DF1%u5733%2CSZQ"})
    browser.cookies.add({"_jc_save_toStation": "%u6B66%u6C49%2CWHN"})
    browser.reload()
    button = browser.find_by_text("查询")
    button.click()
    targetElements = browser.find_by_text("预订")
    targetElements[5].click()

    p2 = browser.find_by_text("冯世杰").last
    p2.click()
    p1 = browser.find_by_text("冯昕").last
    p1.click()

    b_submit = browser.find_by_text("提交订单")
    b_submit.click()



if __name__ == '__main__':
    browser = Browser('firefox')

    do_login(browser)
    do_buy_ticket(browser)

    #browser.quit()





