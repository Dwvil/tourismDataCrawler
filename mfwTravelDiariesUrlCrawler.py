"""
爬取78个目的地的游记地址（url）
总计146201篇游记，将游记的url存入datefile/travel_diaries_url.csv中
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

prefix_td_url = "http://www.mafengwo.cn/yj/"
suffix_td_url = "/1-0-1.html"
mdd_url = "http://www.mafengwo.cn/yj/"
targetUrl = "http://test.abuyun.com"
targetUrl_1 = "http://pv.sohu.com/cityjson"

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
                response = requests.get(page['url'], headers=header, proxies=proxies)
                print(self.threadName, "--", page, "--", response.status_code)
                # if response is not None:
                #     response_data = response.json()
                #     self.dataQueue.put({'spotsID': page['mddid'], 'list_html': response_data['list']})
                #     print(self.threadName, "--获取成功")
                if response.status_code == 200:
                    self.dataQueue.put({'page': page, 'html': response.text})
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
        print("启动" + self.threadName)
        while not PARSE_EXIT:
            try:
                data = self.dataQueue.get(block=True, timeout=10)
                self.parse(data['html'], data['page']['spotsID'], data['page']['scenicSpots'])
                print(self.threadName + "解析完成")
                time.sleep(1)
            except Exception as e:
                print("FROM:", self.threadName, ":", type(e), e)
        print("退出" + self.threadName)

    def parse(self, html, spotsID, scenicSpots):
        soup: BeautifulSoup = BeautifulSoup(html, 'lxml')
        items_a = soup.select("a[class='title-link']")
        with open('./datafile/travel_diaries_url.csv', 'a+', newline="", encoding='utf-8') as csv_file:
            file_header = ['td_id', 'spotsID', 'scenicSpots', 'url']
            csv_file_writer = csv.DictWriter(csv_file, file_header)
            # csv_file_writer.writeheader()
            for item in items_a:
                td_url = item.attrs['href']
                td_id = td_url[td_url.rindex('/') + 1: td_url.rindex('.')]
                csv_file_writer.writerow(
                    {'td_id': td_id, 'spotsID': spotsID, 'scenicSpots': scenicSpots, 'url': td_url})

        # with 后面有两个必须执行的操作：__enter__ 和 _exit__
        # 不管里面的操作结果如何，都会执行打开、关闭
        # 打开锁、处理内容、释放锁
        # with self.lock:
        #     # 写入存储的解析后的数据
        #     self.filename.write(json.dumps(items, ensure_ascii=False).encode("utf-8") + "\n")


# POST_FAILED_URL_DATA = []

CRAWL_EXIT = False
PARSE_EXIT = False
threadLock = threading.Lock()

# 页码的队列
pageQueue = queue.Queue()
# 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
dataQueue = queue.Queue()
# 爬取失败队列
failedQyeye = queue.Queue()

with open('./datafile/travel_diaries_pageCount.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        page_count = int(row['td_pageCount'])
        if page_count >= 190:
            page_count = 150
        for index in range(1, page_count + 1):
            url_combine = "http://www.mafengwo.cn/yj/" + str(row['spotsID']) + "/1-0-" + str(index) + ".html"
            pageQueue.put({
                'url': url_combine,
                'spotsID': row['spotsID'],
                'scenicSpots': row['scenicSpots']
            })
print("待爬取队列大小", pageQueue.qsize())

# 创建抓取线程
crawl_threads = []
for i in range(1, 6):
    thread = ThreadCrawl("抓取Thread-" + str(i), pageQueue, dataQueue)
    thread.start()
    crawl_threads.append(thread)

# 创建解析线程
parse_threads = []
for i in range(1, 3):
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
