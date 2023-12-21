import requests
from bs4 import BeautifulSoup
import re
import csv
from tqdm import tqdm
import pandas as pd
import time
import random
#---------此脚本用于爬取www.poi86.com的poi数据，包括每个poi的地址及大地坐标，数据存储于一个csv表格当中----------
#---------脚本集成了自动清洗数据的功能，清除空行以及不够详细的地址---------

datafile = 'C:/Users/Alienware/Desktop/data.csv'        #此处路径需要修改为你自己的路径，data.csv为存储爬出数据的表格（无需新建）
outputfile = 'C:/Users/Alienware/Desktop/cleaned_data.csv'

with open(datafile, 'w', newline='', encoding='utf-8-sig') as f:       
    datak = {'所属省份': 'a', '所属城市': 'a', '所属区县': 'a', '详细地址': 'a', '大地坐标': 'a', '地名': 'a'}
    writer = csv.DictWriter(f, fieldnames=datak.keys())
    writer.writeheader()  # 写入表头




    startpage = 500
    stoppage = startpage + 10 #从第几页开始爬几页，添加了防ban机制后不用担心被封ip了，实测脚本挂一晚上能爬7k条数据不被ban
    
    for i in tqdm(range(startpage,stoppage + 1), desc='正在逐条爬取数据'):
        # url = 'https://www.poi86.com/poi/amap/' + district[j] + '/' + str(i) + '.html'
        url = "https://www.poi86.com/poi/amap/" + str(i) + ".html"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        } #设置了User-Agent请求头模拟浏览器行为

        for _ in range(3):
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(5)

        soup = BeautifulSoup(response.text, 'html.parser')
        # print(soup)
        links = soup.find_all('a', href=True)
        pattern = re.compile(r'/poi/amap/\d+\.html')

        for link in links:
            if pattern.search(link['href']):
                link_url = 'https://www.poi86.com' + link['href']
                time.sleep(random.randint(1, 5)) #在每次请求之间加入1到5秒随机延时
                for _ in range(3): #防止网络错误，添加重试机制
                    try:
                        response = requests.get(url, headers=headers)
                        if response.status_code == 200:
                            break
                    except requests.RequestException:
                        pass
                    time.sleep(5)
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

                
                h1_tag = soup.find("h1")
                if h1_tag:
                    store_name = h1_tag.text.strip()
                data["地名"] = store_name

            
                writer.writerow(data)  # 写入数据
print("数据爬取完毕！存储于data.csv文件中")

#------洗数据模块，可以单独拿出来用，导pandas包即可------
print('开始清洗数据')
#读CSV文件
df = pd.read_csv(datafile, encoding='utf-8-sig')

#除重复数据
df = df.drop_duplicates()
#除空行
df = df.dropna(how = 'any')

#判断地址是否详细，阈值为4-15，因为大部分不详细地址基本都为xx路，往高了调也可，最大可10，不过会丢不少数据
def is_detailed(address):
    return 4 < len(address) < 15

# 保留详细地址的数据
df = df[df['详细地址'].apply(is_detailed)]

df.to_csv(outputfile, index=False, encoding='utf-8-sig')
print('数据清洗完毕')