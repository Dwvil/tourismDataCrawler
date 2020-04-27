import threading
import time
import csv
import queue
import requests
import re
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import datetime

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

PARSE_EXIT = False


# 抓取驴妈妈各个目的地游记数量
def crawler_diaries_count():
    mdd_list = []
    with open('./datafile_1/lmm_travel_scenic_spots_data.csv', 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            mdd_list.append(row)
    for mdd in mdd_list:
        response = requests.get('http://www.lvmama.com/lvyou/youji/' + mdd['url'], headers=header, proxies=proxies)
        print("抓取驴妈妈目的地游记数量", response.status_code)
        if response.status_code == 200:
            soup: BeautifulSoup = BeautifulSoup(response.text, 'lxml')
            mdd['diariesCount'] = soup.select("div[class='wy_state_page'] p span")[0].text
            print(mdd)
            with open('./datafile_1/lmm_travel_diaries_count_data.csv', 'a+', newline="",
                      encoding='utf-8') as csv_file_1:
                file_header = ['spotsID', 'scenicSpots', 'url', 'diariesCount']
                csv_file_writer = csv.DictWriter(csv_file_1, file_header)
                # csv_file_writer.writeheader()
                csv_file_writer.writerow(mdd)
        time.sleep(0.5)


class ThreadCrawl(threading.Thread):
    def __init__(self, threadName, pageQueue, dataQueue):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.pageQueue = pageQueue
        self.dataQueue = dataQueue

    def run(self):
        print("启动 " + self.threadName)
        while not self.pageQueue.empty():
            try:
                page = self.pageQueue.get(False)
                response = requests.post("http://www.lvmama.com/lvyou/ajax/getTripListNew", data=page['data'],
                                         headers=header)
                print(self.threadName, "--", page, "--", response.status_code)
                if response.status_code == 200:
                    response_data = response.json()
                    self.dataQueue.put({'page': page, 'html': response_data['data']})
                time.sleep(1)
            except Exception as e:
                print(type(e), e)
        print("结束 " + self.threadName)


class ThreadParse(threading.Thread):
    def __init__(self, threadName, dataQueue):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.dataQueue = dataQueue

    def run(self):
        print("启动 " + self.threadName)
        while not PARSE_EXIT:
            try:
                data = self.dataQueue.get(block=True, timeout=20)
                self.parse(data['html'], data['page']['spotsID'], data['page']['scenicSpots'])
                print(self.threadName + "解析完成")
                time.sleep(1)
            except Exception as e:
                print("FROM:", self.threadName, ":", type(e), e)
        print("退出 " + self.threadName)

    def parse(self, html, spotsID, scenicSpots):
        soup: BeautifulSoup = BeautifulSoup(html, 'lxml')
        pattern = '</span>(.*?)发布<span>|</span>(.*?)月出游<span>'
        dd_elements = soup.select("div[class='countryBox'] dl dd")
        with open('./datafile_1/lmm_travel_diaries_detail_data.csv', 'a+', newline="", encoding='utf-8') as csv_file:
            file_header = ['td_url', 'travel_date', 'spotsID', 'scenicSpots']
            csv_file_writer = csv.DictWriter(csv_file, file_header)
            # csv_file_writer.writeheader()
            for dd in dd_elements:
                p = dd.select(".uploadInfo")
                a = dd.select(".title a")
                info_str = re.findall(re.compile(pattern, re.S), str(p))
                # 发布日期
                publication_time = info_str[0][0]
                # 出游日期
                travel_month = int(info_str[1][1])
                date = datetime.datetime.strptime(publication_time, '%Y-%m-%d').date()
                publication_month = int(date.month)
                if publication_month < travel_month:
                    publication_year = int(date.year) - 1
                    date = date.replace(publication_year, travel_month, 1)
                print(date.strftime('%Y-%m'))
                print(a[0].attrs['href'])
                csv_file_writer.writerow(
                    {'td_url': a[0].attrs['href'], 'travel_date': date.strftime('%Y-%m'), 'spotsID': spotsID,
                     'scenicSpots': scenicSpots})


# 抓取驴妈妈各个目的地游记数量，提供给接下来使用
# crawler_diaries_count()

# 待爬取队列
pageQueue = queue.Queue()
# 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
dataQueue = queue.Queue()
with open('./datafile_1/lmm_travel_diaries_count_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        page_count = int(int(row['diariesCount']) / 20) + 1
        for index in range(1, page_count + 1):
            data = {
                'dest_id': re.findall(r'\d+', row['spotsID']),
                'page_size': 20,
                'page_num': index
            }
            pageQueue.put({
                'spotsID': row['spotsID'],
                'scenicSpots': row['scenicSpots'],
                'data': data
            })
print("待爬取队列大小", pageQueue.qsize())
# 创建抓取线程
crawl_threads = []
for i in range(1, 11):
    thread = ThreadCrawl("抓取Thread-" + str(i), pageQueue, dataQueue)
    thread.start()
    crawl_threads.append(thread)

# 创建解析线程
parse_threads = []
for i in range(1, 11):
    thread = ThreadParse("解析Thread-" + str(i), dataQueue)
    thread.start()
    parse_threads.append(thread)

for t in crawl_threads:
    t.join()

while not dataQueue.empty():
    pass
PARSE_EXIT = True

for t in parse_threads:
    t.join()
print("待解析队列大小", dataQueue.qsize())
print("退出主线程")
