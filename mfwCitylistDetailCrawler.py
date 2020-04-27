"""
此脚本使用阿布云代理(HTTP隧道（动态版）)
将每个国家或者地区的热门城市(景点)的参观详情的html（包括一个国家或地区有哪些景点，以及这些景点有多少人去过）
例如：云南的热门城市（地点）地址为http://www.mafengwo.cn/mdd/citylist/12711.html，其中一共有15页，用post请求的方式获取每一页（9个地点）的详情list的html
写入./datafile/scenic_spots_visitedCount_listHtml_data.csv文件，待解析
"""
import threading
import time
import csv
import queue
import requests
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
from requests import exceptions

# 代理服务器
proxyHost = "http-dyn.abuyun.com"
proxyPort = "9020"

# 代理隧道验证信息
proxyUser = "H57451567K539JJD"
proxyPass = "B9FF03F1EEC525F3"

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


class myThread(threading.Thread):
    def __init__(self, threadName, pageQueue, dataQueue):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.pageQueue = pageQueue
        self.dataQueue = dataQueue

    def run(self):
        print("启动 " + self.threadName)
        while not self.pageQueue.empty():
            try:
                ajax_request = 'http://www.mafengwo.cn/mdd/base/list/pagedata_citylist'
                page = self.pageQueue.get(False)
                # print(page['mddid'])
                response = requests.post(ajax_request, data=page, headers=header, proxies=proxies)
                # response_data = response.json()
                # print(self.threadName, "--", response_data['list'])
                if response is not None:
                    response_data = response.json()
                    self.dataQueue.put({'spotsID': page['mddid'], 'list_html': response_data['list']})
                    print(self.threadName, "--获取成功")
                time.sleep(1)
            except Exception as e:
                print(type(e), e)
        print("结束 " + self.threadName)


POST_FAILED_URL_DATA = []


def post_request(url, data, connet_retry_times, status_retry_times=3):
    session = requests.Session()
    session.mount('http://', HTTPAdapter(max_retries=3))
    session.mount('https://', HTTPAdapter(max_retries=3))
    fake_ua = UserAgent()
    headers = {'User-Agent': fake_ua.random}
    try:
        post_response = session.post(url, data=data, headers=headers, timeout=(5, 5))
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


CRAWL_EXIT = False
threadLock = threading.Lock()
threads = []

# 页码的队列
pageQueue = queue.Queue()
# 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
dataQueue = queue.Queue()
with open('./datafile/scenic_spots_pageCount_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        for index in range(1, int(row['pageCount']) + 1):
            pageQueue.put({
                'mddid': row['spotsID'],
                'page': index
            })
print("待爬取队列大小", pageQueue.qsize())

# 创建新线程
for i in range(1, 6):
    thread = myThread("Thread-" + str(i), pageQueue, dataQueue)
    thread.start()
    threads.append(thread)

# 等待pageQueue队列为空，也就是等待之前的操作执行完毕
# while有阻塞主线程的做作用，后面条件成立，pass的意义在于不做任何事，不会往下运行，等上面线程执行完后
# 如果换成一条打印语句（print），则会导致主线程运行，不能起到阻塞主线程的作用
# while not pageQueue.empty():
#     time.sleep(1)
#
# CRAWL_EXIT = True
# 等待所有线程完成
for t in threads:
    t.join()
print("待解析队列大小", dataQueue.qsize())
# 将每个国家或者地区的热门城市(景点)的参观详情的html（包括一个国家或地区有哪些景点，以及这些景点有多少人去过）写入文件，待解析
with open('./datafile/scenic_spots_visitedCount_listHtml_data.csv', 'a+', newline="", encoding='utf-8') as csv_file:
    file_header = ['spotsID', 'list_html']
    csv_file_writer = csv.DictWriter(csv_file, file_header)
    csv_file_writer.writeheader()
    while not dataQueue.empty():
        item = dataQueue.get(False)
        csv_file_writer.writerow({'spotsID': item['spotsID'], 'list_html': item['list_html']})
print("退出主线程")
