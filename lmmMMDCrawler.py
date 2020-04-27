"""
爬取驴妈妈网站上目的地，一共有170个，然后与马蜂窝上的目的地对比，选取相同的写入
"""
import threading
import time
import csv
import queue
import requests
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from requests import exceptions

# 代理服务器
proxyHost = "http-pro.abuyun.com"
proxyPort = "9010"

# 代理隧道验证信息
proxyUser = "H56942H480Q7MNVP"
proxyPass = "8936A10ED4F8EB51"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
}
fake_ua = UserAgent()
header = {
    'User-Agent': fake_ua.random,
    "Proxy-Switch-Ip": "yes"
}
targetUrl = "http://test.abuyun.com"
url = "http://www.lvmama.com/lvyou/"
response = requests.get(url, headers=header, proxies=proxies)
soup: BeautifulSoup = BeautifulSoup(response.text, 'lxml')
tag_a = soup.select("div[class='wy_area_list'] a")
with open('./datafile_1/lmm_travel_scenic_spots_data.csv', 'a+', newline="", encoding='utf-8') as csv_file:
    file_header = ['spotsID', 'scenicSpots', 'url']
    csv_file_writer = csv.DictWriter(csv_file, file_header)
    csv_file_writer.writeheader()
    for a in tag_a:
        mdd_url = a.attrs['href']
        csv_file_writer.writerow(
            {'spotsID': mdd_url[mdd_url.rindex('-') + 1: mdd_url.rindex('.')], 'scenicSpots': a.find("span").text,
             'url': mdd_url})
# 还有后续的与马蜂窝上的目的地对比取相同目的地的操作
