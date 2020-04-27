"""
爬去146201篇游记并解析，将数据存入datafile/travel_diaries_detail.csv中
"""
import threading
import time
import re
import csv
import queue
import requests
import datetime
import execjs
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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


class ThreadCrawl(threading.Thread):
    def __init__(self, threadName, pageQueue, dataQueue, driver):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.pageQueue = pageQueue
        self.dataQueue = dataQueue
        self.driver = driver

    def run(self):
        print("启动 " + self.threadName)
        while not self.pageQueue.empty():
            try:
                # fake_ua = UserAgent()
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
                }
                page = self.pageQueue.get(False)
                response = requests.get(page['url'], headers=headers, proxies=proxies)
                if response.status_code == 521:
                    first_cookie = response.headers['Set-Cookie'].split(';')[0]
                    js_code = re.findall(r'<script>(.*?)</script>', response.text)
                    js = js_code[0]
                    js = js.replace('eval(', 'var data=(')
                    context = execjs.compile(js)
                    data1 = context.eval('data')
                    # 找到cookie生成代码段的开始与结束，把这段生成cookie代码切出来
                    c_start = data1.find('cookie')
                    c_end = data1.find("Path=/;'") + len("Path=/;'")  # 找到位置 + 这几个字符串长度 = 最后的下标元素
                    cookie_code = data1[c_start: c_end]
                    cookie_js = 'var ' + cookie_code + "; return cookie"
                    time.sleep(1)
                    data1 = driver.execute_script(cookie_js)
                    cookie_str = data1.split(';')[0]
                    headers['Cookie'] = first_cookie + ';' + cookie_str
                    response_200 = requests.get(page['url'], headers=headers, proxies=proxies)
                    print(self.threadName, "--", page, "--", response_200.status_code)
                    if response_200.status_code == 200:
                        self.dataQueue.put({'page': page, 'html': response_200.text})
                    if response_200.status_code == 429:
                        self.pageQueue.put(page)
                if response.status_code == 200:
                    print(self.threadName, "--", page, "--", response.status_code)
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
                self.parse(data['html'], data['page']['td_id'], data['page']['spotsID'], data['page']['scenicSpots'])
                print(self.threadName + "解析完成")
                time.sleep(1)
            except Exception as e:
                print("FROM:", self.threadName, ":", type(e), e)
        print("退出" + self.threadName)

    def parse(self, html, td_id, spotsID, scenicSpots):
        soup: BeautifulSoup = BeautifulSoup(html, 'lxml')
        time_li = soup.select("li[class='time']")
        day_li = soup.select("li[class='day']")
        people_li = soup.select("li[class='people']")
        cost_li = soup.select("li[class='cost']")
        time = 'null'
        day = 'null'
        people = 'null'
        cost = 'null'
        if len(time_li) > 0:
            time = time_li[0].get_text()
        if len(day_li) > 0:
            day = day_li[0].get_text()
        if len(people_li) > 0:
            people = people_li[0].get_text()
        if len(cost_li) > 0:
            cost = cost_li[0].get_text()
        with open('./datafile/travel_diaries_detail.csv', 'a+', newline="", encoding='utf-8') as csv_file:
            file_header = ['td_id', 'spotsID', 'scenicSpots', 'time', 'day', 'people', 'cost']
            csv_file_writer = csv.DictWriter(csv_file, file_header)
            # csv_file_writer.writeheader()
            csv_file_writer.writerow(
                {'td_id': td_id, 'spotsID': spotsID, 'scenicSpots': scenicSpots, 'time': time, 'day': day,
                 'people': people, 'cost': cost})
        # with 后面有两个必须执行的操作：__enter__ 和 _exit__
        # 不管里面的操作结果如何，都会执行打开、关闭
        # 打开锁、处理内容、释放锁
        # with self.lock:
        #     # 写入存储的解析后的数据
        #     self.filename.write(json.dumps(items, ensure_ascii=False).encode("utf-8") + "\n")


chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
print("打开浏览器前", datetime.datetime.now())
driver = webdriver.Chrome(executable_path="./driver/chromedriver.exe", chrome_options=chrome_options)
print("打开浏览器后", datetime.datetime.now())

CRAWL_EXIT = False
PARSE_EXIT = False
threadLock = threading.Lock()

# 页码的队列
pageQueue = queue.Queue()
# 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制
dataQueue = queue.Queue()
# 爬取失败队列
failedQyeye = queue.Queue()

with open('./datafile/travel_diaries_url.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        td_url = "http://www.mafengwo.cn/" + row['url']
        pageQueue.put({
            'url': td_url,
            'td_id': row['td_id'],
            'spotsID': row['spotsID'],
            'scenicSpots': row['scenicSpots']
        })
print("待爬取队列大小", pageQueue.qsize())

# 创建抓取线程
crawl_threads = []
for i in range(1, 16):
    thread = ThreadCrawl("抓取Thread-" + str(i), pageQueue, dataQueue, driver)
    thread.start()
    crawl_threads.append(thread)

# 创建解析线程
parse_threads = []
for i in range(1, 5):
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
driver.close()
print("待解析队列大小", dataQueue.qsize())
print("退出主线程")
