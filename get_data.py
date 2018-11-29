import requests
from bs4 import BeautifulSoup
import logging.handlers
from sql_models import DB, apps_version_record
import time
import datetime
import re
from selenium import webdriver


def get_app_names(platform='Android', is_china=True):
    # 返回app_id列表
    sql_base = '''SELECT raw_db.apps.app_id FROM raw_db.apps
                        left OUTER JOIN raw_db.apps_version_record
                        ON raw_db.apps.app_id = raw_db.apps_version_record.app_id where platform = '{}' and area = '{}' ;'''
    couture = 'China' if is_china else 'overseas'
    sql = sql_base.format(platform, couture)
    cursor = session.execute(sql)
    datas = cursor.fetchall()
    app_ids = []
    for app_id in datas:
        app_ids.append(app_id[0])
    session.commit()
    return app_ids


def Android_China(app_ids):
    # 连续抓取5次出错之后，就重启。
    app_dicts = []
    error_num = 0
    while True:
        if len(app_ids) == 0:
            break
        app_id = app_ids.pop()
        try:
            logging.info('start crawler：' + str(app_id))
            app_dict_get = {}
            url = 'https://android.myapp.com/myapp/detail.htm?apkName=' + str(app_id)
            response = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(response.text, 'lxml')
            app_name = soup.find('div', attrs={'class': 'det-name-int'}).text
            rate = soup.find('div', attrs={'class': 'com-blue-star-num'}).text[:-1]
            category = soup.find('a', attrs={'class': 'det-type-link'}).text
            size = soup.find('div', attrs={'class': 'det-size'}).text
            info = soup.find_all('div', attrs={'class': 'det-othinfo-data'})
            version = info[0].text[1:]
            publisher = info[2].text
            note = soup.find_all('div', attrs={'class': 'det-app-data-info'})[1].text
            update_time = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(info[1]['data-apkpublishtime']))))
            # 存入数据队列
            app_dict_get['url'] = url
            app_dict_get['app_name'] = app_name
            app_dict_get['app_id'] = app_id
            app_dict_get['rate'] = rate
            app_dict_get['version'] = version
            app_dict_get['size'] = size
            app_dict_get['publisher'] = publisher
            app_dict_get['note'] = note
            app_dict_get['category'] = category
            app_dict_get['update_time'] = update_time
            logging.info("{} crawler end".format(app_id))
            app_dicts.append(app_dict_get)
            # time.sleep(5)
        except Exception as e:
            # 发送邮件
            if error_num > 4:
                break
            error_num = error_num + 1
            app_ids.append(app_id)
            logging.error(e)
            time.sleep(5)
    save_interface(app_dicts)


def Android_oversea(app_ids):
    # 连续抓取5次出错之后，就重启。
    app_dicts = []
    error_num = 0
    while True:
        if len(app_ids) == 0:
            break
        app_id = app_ids.pop()
        try:
            logging.info('start crawler：' + str(app_id))
            app_dict_get = {}

            url = 'https://play.google.com/store/apps/details?id=' + str(app_id) + '&hl=en'
            r = requests.get(url, headers=HEADERS).text
            soup = BeautifulSoup(r, 'lxml')
            app_name = soup.find('h1', attrs={'class': 'AHFaub'}).text
            rate = soup.find('div', attrs={'class': 'BHMmbe'}).text
            category = soup.find('a', attrs={'itemprop': 'genre'}).text
            try:
                note = soup.find_all('div', attrs={'jsname': 'bN97Pc'})[1].text.strip()
            except:
                note = soup.find('div', attrs={'jsname': 'bN97Pc'}).text.strip()
            info = soup.find_all('span', attrs={'class': 'htlgb'})
            update_time = info[0].text.replace(', ', ' ').split(' ')[2] + '-' + \
                          info[0].text.replace('November', '11').replace('December', '12').replace('October',
                                                                                                   '10').replace(
                              'September', '9').replace('August', '8').replace('July', '11').replace('June',
                                                                                                     '11').replace(
                              'May', '11').replace('April', '11').replace('March', '11').replace('February',
                                                                                                 '11').replace(
                              'January', '11').replace(', ', ' ').split(' ')[0] + '-' + \
                          info[0].text.replace('November', '11').replace(', ', ' ').split(' ')[1]
            size = info[2].text
            version = info[6].text
            publisher = info[18].text

            # 存入数据队列
            app_dict_get['url'] = url
            app_dict_get['app_name'] = app_name
            app_dict_get['app_id'] = app_id
            app_dict_get['rate'] = rate
            app_dict_get['version'] = version
            app_dict_get['size'] = size
            app_dict_get['publisher'] = publisher
            app_dict_get['note'] = note
            app_dict_get['category'] = category
            app_dict_get['update_time'] = update_time
            logging.info("{} crawler end".format(app_id))
            app_dicts.append(app_dict_get)
            # time.sleep(5)
        except Exception as e:
            # 发送邮件
            if error_num > 4:
                break
            error_num = error_num + 1
            app_ids.append(app_id)
            logging.error(e)
            time.sleep(5)
    save_interface(app_dicts)


def ios_get_base_info(web_page,app_dict_get):
    soup = BeautifulSoup(web_page, 'lxml')
    app_name = soup.find('div', attrs={'class': 'appname'}).text.strip()
    app_dict_get['app_name']=app_name
    category = soup.find('div', attrs={'class': 'genre'}).text.replace("分类","")
    app_dict_get['category']=category
    publisher = soup.find('p', attrs={'class':'info app-developer'}).text
    app_dict_get['publisher']=publisher
    lis=soup.find_all('p', attrs={'class':'info'})
    version_x=True#标记version有没有被使用过，使用过就不记录
    for li in lis:
        if version_x and re.search(r'\d+\.\d+\.\d+',li.text):
            version=li.text
            app_dict_get['version']=version
            version_x=False
        if re.search(r'^[\d.M]+$',li.text) and "M" in li.text:
            size=li.text
            print(size)
            app_dict_get['size']=size
        if re.search(r'\d+-\d+-\d+',li.text):
            update_time=li.text
            app_dict_get['update_time']=update_time
    print(app_dict_get)
    return app_dict_get

def ios_get_update(web_page,app_dict_get):
    soup = BeautifulSoup(web_page, 'lxml')
    note = soup.find('div',attrs={'class':'note app-describe app-describe-showTxt'}).text
    print(note)
    app_dict_get['note']=note
    return app_dict_get


def ios(app_ids,oversea=True):
    # 连续抓取5次出错之后，就重启。
    app_dicts = []
    error_num = 0
    country= 'us' if oversea else 'cn'
    while True:
        if len(app_ids) == 0:
            break
        app_id = app_ids.pop()
        try:
            logging.info('start crawler：' + str(app_id))
            app_dict_get = {}


            app_name = ''
            rate = ''
            category = ''
            note = ''
            update_time = ''
            size = ''
            version = ''
            publisher = ''

            # 存入数据队列
            url = 'https://www.qimai.cn/app/globalRank/appid/{}/country/{}'.format(app_id, country)
            app_dict_get['url'] = url
            app_dict_get['app_name'] = app_name
            app_dict_get['app_id'] = app_id
            app_dict_get['rate'] = rate
            app_dict_get['version'] = version
            app_dict_get['size'] = size
            app_dict_get['publisher'] = publisher
            app_dict_get['note'] = note
            app_dict_get['category'] = category
            app_dict_get['update_time'] = update_time

            driver = webdriver.Remote(
                command_executor='localhost:30000/wd/hub',
                desired_capabilities={'browserName': 'chrome'},
            )
            base_info_url='https://www.qimai.cn/app/baseinfo/appid/{}/country/{}'.format(app_id, country)
            driver.get(base_info_url)
            driver.save_screenshot("base-info.png")
            with open("r.html","w") as f:
                f.write(driver.page_source)
            time.sleep(5)
            app_dict_get=ios_get_base_info(driver.page_source,app_dict_get)
            update_url='https://www.qimai.cn/app/version/appid/{}/country/{}'.format(app_id, country)
            driver.get(update_url)
            time.sleep(5)
            app_dict_get=ios_get_update(driver.page_source,app_dict_get)
            get_rate_url='https://www.qimai.cn/app/comment/appid/{}/country/{}'.format(app_id, country)
            driver.get(get_rate_url)
            time.sleep(5)
            num = driver.find_element_by_class_name('num')
            app_dict_get['rate']=num.text

            # driver.save_screenshot("codingpy.png")
            driver.close()
            url = 'https://www.qimai.cn/app/globalRank/appid/{}/country/{}'.format(app_id, country)#最后展示的数据
            app_dict_get['url'] = url
            print(app_dict_get)
            logging.info("{} crawler end".format(app_id))
            app_dicts.append(app_dict_get)
            # time.sleep(5)
        except Exception as e:
            driver.close()
            if error_num > 4:
                break
                # 发送邮件
            error_num = error_num + 1
            app_ids.append(app_id)
            logging.error(e)
            raise
            time.sleep(5)
    save_interface(app_dicts)



def remind(app_name, version, note, url):
    item = 'app_name:{}\nversion:{}\nnote:{}\n获取内容所在链接:{}'.format(app_name, version, note, url)
    data = {"msgtype": "text", "text": {"content": item}}
    # requests.post(
    #     'https://oapi.dingtalk.com/robot/send?access_token=22678ee70e3b2bc36fa7b61788ddf8505fa668d492c3d20eb98a1e1d6cc5ab75',
    #     json=data)


def save_interface(post_dicts):
    for post_dict in post_dicts:
        app_id = post_dict['app_id']
        app_name = post_dict['app_name']
        publisher = post_dict['publisher']
        version = post_dict['version']
        size = post_dict['size']
        category = post_dict['category']
        rate = post_dict['rate']
        note = post_dict['note']
        url = post_dict['url']
        update_time = post_dict['update_time']

        new_post = session.query(apps_version_record).filter_by(app_id=app_id).first()
        if new_post:
            if version != new_post.version:
                setattr(new_post, 'app_id', app_id)
                setattr(new_post, 'app_name', app_name)
                setattr(new_post, 'publisher', publisher)
                setattr(new_post, 'version', version)
                setattr(new_post, 'size', size)
                setattr(new_post, 'category', category)
                setattr(new_post, 'rate', rate)
                setattr(new_post, 'note', note)
                setattr(new_post, 'url', url)
                setattr(new_post, 'update_time', update_time)

                remind(app_name=app_name, note=note, version=version, url=url)
                logging.info('update app_id:{}'.format(app_id))
        else:  # insert
            new_post = apps_version_record(app_id=app_id, app_name=app_name, publisher=publisher
                                           , version=version, size=size, category=category,
                                           rate=rate, note=note, url=url, update_time=update_time)
            session.add(new_post)
    session.commit()


def run():
    try:
        android_china = get_app_names(platform='Android', is_china=True)
        # Android_China(android_china)
        android_overseas = get_app_names(platform='Android', is_china=False)
        # Android_oversea(android_overseas)
        ios_china = get_app_names(platform='iOS', is_china=True)
        ios(app_ids=ios_china,oversea=False)
        ios_overseas = get_app_names(platform='iOS', is_china=False)
        ios(app_ids=ios_overseas,oversea=True)
    except Exception as e:
        logging.error(e)
        #发送邮件

if __name__ == '__main__':
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    error_num = 0  # 记载出错次数，
    logging.basicConfig(
        handlers=[logging.handlers.RotatingFileHandler('./apps.log', maxBytes=20 * 1024 * 1024, backupCount=5,
                                                       encoding='utf-8'),
                  logging.StreamHandler()  # 供输出使用
                  ],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s %(filename)s %(funcName)s %(lineno)s - %(message)s"
    )
    session = DB().session
    # get_app_names()
    run()
