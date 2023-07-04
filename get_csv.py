import requests
from bs4 import BeautifulSoup
import re
import csv
from tqdm import tqdm
import pandas as pd
#---------此脚本用于爬取www.poi86.com的poi数据，包括每个poi的地址及大地坐标，数据存储于一个csv表格当中----------
#---------脚本集成了自动清洗数据的功能，清除空行以及不够详细的地址---------


with open('C:/Users/Alienware/Desktop/data.csv', 'w', newline='', encoding='utf-8-sig') as f:
    datak = {'所属省份': 'a', '所属城市': 'a', '所属区县': 'a', '详细地址': 'a', '大地坐标': 'a'}
    writer = csv.DictWriter(f, fieldnames=datak.keys())
    writer.writeheader()  # 写入表头
    # poi86网站中南京市各区的网页代码，例：https://www.poi86.com/poi/amap/district/320118/1.html 320118即爬取南京市高淳区的poi数据
    # 根据需求自行修改此列表即可
    district = ['320102','320104','320105','320106','320111','320113','320114','320115','320116','320117','320118']
    master_page = len(district)
    for j in range(0,master_page):
        print('开始爬取第%d个区的数据，共%d个区'%(j +1,master_page + 1))
        page = 20 #每个区爬取多少页数据，最多20页，往后都是空
        for i in tqdm(range(1,page + 1), desc='正在逐页爬取数据'):
            url = 'https://www.poi86.com/poi/amap/district/' + district[j] + '/' + str(i) + '.html'
            response = requests.get(url)

            soup = BeautifulSoup(response.text, 'html.parser')

            links = soup.find_all('a', href=True)
            pattern = re.compile(r'/poi/amap/\d+\.html')

            for link in links:
                if pattern.search(link['href']):
                    link_url = 'https://www.poi86.com' + link['href']
                    response = requests.get(link_url)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    # print(soup)
                    data = {}
                    for item in soup.select('ul li'):
                        text = item.text.strip()
                        # print(text)
                        if "所属省份:" in text:
                            data["所属省份"] = text.split(":")[1].strip()
                        elif "所属城市:" in text:
                            data["所属城市"] = text.split(":")[1].strip()
                        elif "所属区县:" in text:
                            data["所属区县"] = text.split(":")[1].strip()
                        elif "详细地址:" in text:
                            data["详细地址"] = text.split(":")[1].strip()
                        elif "大地坐标:" in text:
                            data["大地坐标"] = text.split(":")[1].strip()
                    # print(data)

                    writer.writerow(data)  # 写入数据
print("数据爬取完毕！存储于data.csv文件中")

#------洗数据模块，可以单独拿出来用，导pandas包即可------
print('开始清洗数据')
#读CSV文件
df = pd.read_csv('C:/Users/Alienware/Desktop/data.csv', encoding='utf-8-sig')

#除重复数据
df = df.drop_duplicates()
#除空行
df = df.dropna(how = 'any')

#判断地址是否详细，阈值为3，因为大部分不详细地址基本都为xx路，往高了调也可，最大可10，不过会丢不少数据
def is_detailed(address):
    return len(address) > 3

# 保留详细地址的数据
df = df[df['详细地址'].apply(is_detailed)]

df.to_csv('C:/Users/Alienware/Desktop/cleaned_data.csv', index=False, encoding='utf-8-sig')
print('数据清洗完毕')