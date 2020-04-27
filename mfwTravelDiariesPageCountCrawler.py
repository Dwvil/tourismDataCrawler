"""
爬取78个目的地中每个目的地的游记页数，
如http://www.mafengwo.cn/yj/12711/ 云南热门游记共有189页（一页15片游记）
"""
import csv
import pandas
import time
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

"""td = travel diaries"""
prefix_td_url = "http://www.mafengwo.cn/yj/"
suffix_td_url = "/1-0-1.html"

mdd_url = "http://www.mafengwo.cn/yj/"
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
header = {'User-Agent': fake_ua.random}


def crawler_parse(spotsID, scenicSpots):
    response = requests.get(mdd_url + spotsID, headers=header, proxies=proxies)
    soup: BeautifulSoup = BeautifulSoup(response.text, 'lxml')
    with open('./datafile/travel_diaries_pageCount.csv', 'a+', newline="", encoding='utf-8') as csv_file:
        file_header = ['spotsID', 'scenicSpots', 'td_pageCount']
        csv_file_writer = csv.DictWriter(csv_file, file_header)
        # csv_file_writer.writeheader()
        csv_file_writer.writerow({'spotsID': spotsID, 'scenicSpots': scenicSpots,
                                  'td_pageCount': int(soup.select("span[class='count'] span")[0].text)})
    print(soup.select("span[class='count'] span")[0].text)


if __name__ == '__main__':
    with open('./datafile/travel_scenic_spots_data.csv', 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            crawler_parse(row['spotsID'], row['scenicSpots'])
            time.sleep(1)
