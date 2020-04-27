"""
解析mfwCitylistDetailCrawler(datafile/scenic_spots_visitedCount_listHtml_data.csv)中获取的结果
"""
import csv
import pandas
from bs4 import BeautifulSoup


def csv_read():
    i = 0
    dict_list = []
    with open('./datafile/scenic_spots_visitedCount_listHtml_data.csv', 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            dict_list.append(row)
            # print(type(row))
            # print(row)
    return dict_list


def parse(id, html):
    soup: BeautifulSoup = BeautifulSoup(html, 'lxml')
    items_li = soup.select("li[class='item']")
    with open('./datafile/scenic_spots_visitedCount_detail_data.csv', 'a+', newline="", encoding='utf-8') as csv_file:
        file_header = ['spotsID', 'spotsName', 'spotsVisitCount']
        csv_file_writer = csv.DictWriter(csv_file, file_header)
        # csv_file_writer.writeheader()
        for item in items_li:
            spotsName = item.find("div", class_="title").contents[0].replace('\n', '').replace(' ', '')
            spotsVisitCount = item.select("div[class='nums'] b")[0].text
            csv_file_writer.writerow({'spotsID': id, 'spotsName': spotsName, 'spotsVisitCount': int(spotsVisitCount)})


if __name__ == '__main__':
    dict_list = csv_read()
    # df = pandas.DataFrame([{'Name': 'C', 'Age': 10}, {'Name': 'D', 'Age': 20}])
    # df = pandas.DataFrame(dict_list)
    # df_loc = df.loc[(df['spotsID'] == '11131')]
    # print(df_loc.loc[434].values[0:-1])
    # print(df_loc.loc[434].values)
    # # DataFrame转字典
    # data = df_loc.to_dict(orient='records')
    for i in dict_list:
        print(i['spotsID'])
        parse(i['spotsID'], i['list_html'])
