import csv
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

ip_generic = 'http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=a50db5d898384db6b1fb4475d8831b7b&count=10&expiryDate=0&format=1&newLine=2'
fake_ua = UserAgent()
header = {'User-Agent': fake_ua.random}
# proxy = {
#     # 'http': '180.114.176.228:27055',
#     'https': '222.160.106.149:30017'
# }
url = 'https://www.mafengwo.cn/mdd/'
try:
    response = requests.get(url, headers=header, timeout=3)
    # response = requests.get(url, headers=header, proxies=proxy, timeout=3)
    soup: BeautifulSoup = BeautifulSoup(response.text, 'lxml')
    scenicSpots_result = soup.select(".hot-list .col dl dt a")
    scenicSpots_special_elements = soup.select(".hot-list .col dl dd")
    with open('./datafile/travel_scenic_spots_data.csv', 'a+', newline="", encoding='utf-8') as csv_file:
        file_header = ['spotsID', 'scenicSpots', 'url', 'type']
        csv_file_writer = csv.DictWriter(csv_file, file_header)
        csv_file_writer.writeheader()
        for scenicSpot in scenicSpots_result:
            url = scenicSpot.attrs['href']
            csv_file_writer.writerow(
                {'spotsID': url[url.rindex('/') + 1: url.rindex('.')], 'scenicSpots': scenicSpot.text, 'url': url,
                 'type': 'normal'})
        for i in scenicSpots_special_elements:
            if i.parent.find("dt").find("a") is None:
                for scenicSpots_special in i.parent.find("dd").find_all("a"):
                    url2 = scenicSpots_special.attrs['href']
                    csv_file_writer.writerow(
                        {'spotsID': url2[url2.rindex('/') + 1: url2.rindex('.')],
                         'scenicSpots': scenicSpots_special.text,
                         'url': url2,
                         'type': 'special'})
except Exception as e:
    print(e)
