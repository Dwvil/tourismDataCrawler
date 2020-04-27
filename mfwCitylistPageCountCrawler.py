"""
此脚本用于爬取从mfwMMDCrawler.py获取到的目的地（国家或者地区）结果的热门城市（地点）的页数
例如：云南的热门城市（地点）地址为http://www.mafengwo.cn/mdd/citylist/12711.html，其中一共有15页，一页九个
"""
import csv
import time
import requests
from bs4 import BeautifulSoup
from requests import exceptions
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent

# 每个国家或者地区的热门城市(景点)
prefix_html_request = 'http://www.mafengwo.cn/mdd/citylist/'
suffix_html_request = '.html'
ajax_request = 'http://www.mafengwo.cn/mdd/base/list/pagedata_citylist'
# 蘑菇代理的隧道订单
appKey = "aWJ0WGcxYmZLcEF2ek94czpHR1RKWkx2WHRxZERCOVMz"
# 蘑菇隧道代理服务器地址
ip_port = 'secondtransfer.moguproxy.com:9001'
proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
headers = {
    "Proxy-Authorization": 'Basic ' + appKey,
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
    "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4"}

POST_FAILED_URL_DATA = []
GET_FAILED_URL = []


# 暂时没有用到
def post_request(url, data, connet_retry_times, status_retry_times=3):
    session = requests.Session()
    session.mount('http://', HTTPAdapter(max_retries=3))
    session.mount('https://', HTTPAdapter(max_retries=3))
    try:
        post_response = session.post(url, data=data, headers=headers, proxies=proxy, verify=False,
                                     allow_redirects=False, timeout=(5, 5))
        if post_response.status_code == 404:
            post_response = None
            POST_FAILED_URL_DATA.append(data)
            print("status_code:" + str(post_response.status_code))
        # 服务器故障时重试
        if 500 <= post_response.status_code < 600:
            post_response = None
            if status_retry_times > 0:
                # time.sleep(1)
                post_request(url, status_retry_times - 1)
    except Exception as e:
        post_response = None
        print(e)
        # 连接失败时重试
        if isinstance(e, exceptions.ConnectionError):
            if connet_retry_times > 0:
                # time.sleep(1)
                print("retry_times:", connet_retry_times)
                post_request(url, connet_retry_times - 1)
    return post_response


def get_request(url, connet_retry_times, status_retry_times=3):
    session = requests.Session()
    session.mount('http://', HTTPAdapter(max_retries=3))
    session.mount('https://', HTTPAdapter(max_retries=3))
    try:
        get_response = session.get(url, headers=headers, proxies=proxy, verify=False, allow_redirects=False,
                                   timeout=(5, 5))
        html = get_response.text
        # print("status_code:" + str(response.status_code))
        # 页面不存在
        if get_response.status_code == 404:
            html = None
            GET_FAILED_URL.append(url)
            print("status_code:" + str(get_response.status_code))
        # 服务器故障时重试
        if 500 <= get_response.status_code < 600:
            html = None
            if status_retry_times > 0:
                # time.sleep(1)
                get_request(url, status_retry_times - 1)
    except Exception as e:
        html = None
        print(e)
        # 连接失败时重试
        if isinstance(e, exceptions.ConnectionError):
            if connet_retry_times > 0:
                # time.sleep(1)
                print("retry_times:", connet_retry_times)
                get_request(url, connet_retry_times - 1)
    return html


# 爬取从mfwMMDCrawler.py获取到的目的地（国家或者地区）结果的热门城市（地点）的页数
# 例如：云南的热门城市（地点）地址为http://www.mafengwo.cn/mdd/citylist/12711.html，其中一共有15页，一页九个
def scenic_spots_page_count():
    with open('./datafile/travel_scenic_spots_data.csv', 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        print(type(reader))
        page_count = -1
        for row in reader:
            print(row['spotsID'], row['scenicSpots'])
            response = get_request("http://www.mafengwo.cn/mdd/citylist/" + row['spotsID'] + ".html", 3)
            if response is not None:
                soup: BeautifulSoup = BeautifulSoup(response, 'lxml')
                pages_temp = soup.select("span[class='count']")[0].text;
                pages = filter(str.isdigit, pages_temp)
                page_count = int("".join(pages))
                print(page_count)
            else:
                page_count = -1
                print(page_count)
            with open('./datafile/scenic_spots_pageCount_data.csv', 'a+', newline="", encoding='utf-8') as csv_file_2:
                file_header = ['spotsID', 'scenicSpots', 'pageCount']
                csv_file_writer = csv.DictWriter(csv_file_2, file_header)
                # csv_file_writer.writeheader()
                csv_file_writer.writerow(
                    {'spotsID': row['spotsID'], 'scenicSpots': row['scenicSpots'], 'pageCount': page_count})


if __name__ == '__main__':
    scenic_spots_page_count()
