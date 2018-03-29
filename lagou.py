import json
import uuid
from multiprocessing import Pool

import requests
import time
from bs4 import BeautifulSoup
from pymongo import MongoClient


def get_uuid():
    return str(uuid.uuid4())

client = MongoClient('localhost', 27017)
db = client.Lagou

header = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Content-Length": "26",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": "JSESSIONID="+get_uuid(),
    "Host": "www.lagou.com",
    "Origin": "https://www.lagou.com",
    "Referer": "https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput=",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36",
    "X-Anit-Forge-Code": "0",
    "X-Anit-Forge-Token": "None",
    "X-Requested-With": "XMLHttpRequest"
}


def request_url(offset):
    url = "https://www.lagou.com/jobs/positionAjax.json"

    payload = {'px': 'new',
               'needAddtionalResult': 'false',
               'isSchoolJob': '0'
               }
    formdata = {'first': 'false',
                'pn': offset,
                'kd': 'python'
                }

    try:
        response = requests.request("POST", url, data=formdata, params=payload, headers=header)
        if response.status_code == 200:
            return response.json()
        return None
    except ConnectionError:
        print('connect error')
        return None


def get_detailID(origin_json):
    if "content" in origin_json.keys():
        for item in origin_json["content"]["hrInfoMap"]:
            yield item


def requset_detail(ID):
    url = "https://www.lagou.com/jobs/{ID}.html"
    header = {
        "User-Agent": "Mozilla / 5.0(Macintosh;IntelMacOSX10_13_3) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 65.0.3325.162Safari / 537.36",
        "Cookie": "JSESSIONID = " + get_uuid()
    }
    try:
        respons = requests.get(url.format(ID=ID), headers=header)
        if respons.status_code == 200:
            return  respons.text
        return None
    except ConnectionError:
        print('url error', url)
        return None

def parse_detail(html,ID):
    soup = BeautifulSoup(html,'lxml')
    name = soup.find('div',class_="job-name")
    company = soup.find('img',class_='b2')
    industry = soup.select('#job_company > dd > ul > li')
    INFO = {"ID": ID, "NAME":'',"COMPANY":'',"INDUSTRY":''}
    if name :
        INFO["ID"]=ID
        INFO["NAME"]=name.get('title')
        INFO["COMPANY"]=company.attrs['alt']
        INFO["INDUSTRY"]=industry[0].get_text()[:-3].strip(' \s \n')


    else:
        print("error in INFO" , ID )
        return False
    request = soup.select('body > div.position-head > div > div.position-content-l > dd > p > span')
    Request = {"SALARY": '', "ADDRESS": '' , "EXPERIENCE": '','EDUCATION':'', "NATURE":''}

    for key,item in zip(Request,request):
        if item :
            Request[key]=item.get_text().strip('/ ')
        else:
            print("error in request" , ID)
            return False
    job_detail = soup.select('#job_detail > dd.job_bt > div > p',)
    # job_detail_l = soup.select('#job_detail > dd.job_bt > div > ol > li:nth-child(1) > p')
    # job_detail = job_detail + job_detail_l
    content = ''
    for detail in job_detail:
        text = detail.get_text()
        content = content + text

    INFO.update(Request)
    INFO['DETAIL'] = content
    return INFO

def mongodb(doc, db,ID):

    if db.lagou_info.find({'ID':ID}).count == 1:

        print(db.lagou_info.find({'ID':ID}).count)
        print("it have already exixt !")

    else:
        if db['lagou'].update({'ID': doc['ID']}, {'$set': doc }, True):
            print(db.lagou_info.find({'ID':ID}))
            print("save to mongo succsee")
            return True
        else:
            print("save False")
            return False

def main(offset):

    for id in get_detailID(request_url(offset)):
        html = requset_detail(id)
        time.sleep(2)
        doc = parse_detail(html, id)
        global a
        a += 1
        print(offset,a,doc)
        if doc:
            mongodb(doc, db,id)
        else:
            print(" 爬虫失败 ： "+ id + "\n")

if __name__ == '__main__':

    a = 1
    pageCount = request_url(1)['content']['positionResult']['totalCount'] // 15
    print(pageCount)
    for page in range(1,pageCount+1):
        try:
            if page%30 == 0:
                time.sleep(30)
            else:
                main(page)
        except:
            print("something happend")
    # pool = Pool()
    # group = [x for x in range(1,pageCount+1)]
    # pool.map(main,group)


