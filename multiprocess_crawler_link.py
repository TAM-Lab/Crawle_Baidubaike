"""
2020-5-12

爬取百度百科每个实体页面下的所有a标签(表示link标签)
"""
import os
import re
import json
import time
import requests
import random
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse
from collections import defaultdict
from multiprocessing import Process, Queue, Manager, Lock

def construct_url(keyword):
    baseurl = 'https://baike.baidu.com/item/'
    url = baseurl + str(keyword)
    return url

def main_crawler(url, yixiang, request_headers):
    """对知识库中的每一个subject，访问其百度百科页面，然后获取页面下所有的超链接"""
    link_list = []
    while True:
        try:
            response = requests.get(url, headers=request_headers)
            break
        except:
            print("#####Please wait 10 seconds#####")
            time.sleep(10)
    print(response.url, response.status_code)
    req_text = response.text
    soup = BeautifulSoup(req_text, 'lxml')
    if soup.find('div', attrs={'class': 'lemmaWgt-subLemmaListTitle'}):
        # 说明页面进入到多义词列表的页面，需要从多义词列表中找到匹配待检索实体的义项描述的链接
        li_label = soup.find_all('li', attrs={'class': 'list-dot list-dot-paddingleft'})
        for li in li_label:
            para_label = li.find('div', attrs={'class': 'para'})
            text = para_label.get_text()
            res = text.split('：')[-1]
            if res == yixiang:
                a_label = li.find('a')
                redirect_url = "https://baike.baidu.com" + a_label['href']
                # 重新获取到该subject对应页面的链接地址，访问获取到页面下的所有超链接
                entity_link_list = get_page_link(redirect_url, request_headers)
                link_list = iterate_all_page_links(entity_link_list, request_headers)
                return link_list
    elif soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'}):
        # 说明进入了对应subject的百科页面，如果是多义词找到对应义项描述的链接网页
        ul_label = soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'})
        li_label = ul_label.find_all('li', attrs={'class': 'item'})
        for li in li_label:
            text = li.get_text().strip('▪')
            if text == yixiang:
                if not li.find('a'):
                    # 未发现a标签，则说明是当前页面下，则直接使用原始的Url链接
                    entity_link_list = get_page_link(url, request_headers)
                    link_list = iterate_all_page_links(entity_link_list, request_headers)
                    return link_list
                else:
                    # 否则获取新的重定向链接
                    a_label = li.find('a')
                    redirect_url = "https://baike.baidu.com" + a_label['href']
                    entity_link_list = get_page_link(redirect_url, request_headers)
                    link_list = iterate_all_page_links(entity_link_list, request_headers)
                    return link_list
    elif soup.find('dd', attrs={'class':'lemmaWgt-lemmaTitle-title'}):
        # 如果subject对应的页面是单义词，则直接获取页面下的所有超链接
        entity_link_list = get_page_link(url, request_headers)
        link_list = iterate_all_page_links(entity_link_list, request_headers)
        return link_list
    else:
        # 可能是未知页面，返回None
        # 存在是多义词，在其义项描述不在百度百科多义词列表里。
        return None

    if len(link_list) == 0:
        # 说明百度百科页面中没有相应的义项描述与之对应，返回None
        return None



def get_page_link(url, request_headers):
    # entity_link_dict = defaultdict(list)
    entity_link_list = []
    while True:
        try:
            req_text = requests.get(url, headers=request_headers).text
            break
        except:
            print("#####Please wait 10 seconds#####")
            time.sleep(10)
    soup = BeautifulSoup(req_text, 'lxml')
    main_content = soup.find('div', attrs={'class':'main-content'})
    a_label = main_content.find_all('a', attrs={'target':'_blank'})
    for a_tag in a_label:
        try:
            href = a_tag['href']
            href_val = validate_href(href)
            if not href_val:
                continue
            link_href = href
            entity_link_list.append(link_href)
        except:
            continue

    return entity_link_list

def iterate_all_page_links(link_list, request_headers):
    """对于某一个页面下的所有超链接，获取每个超链接的href、页面title和义项描述"""
    title_href = "https://baike.baidu.com"
    baike_id_list = []
    link_data = []
    for link in link_list:
        item_dict = dict()
        # baike_id = link.split('/')[-1]  # 获取该实体在百度百科知识库中的id
        if link in baike_id_list:
            # 避免访问重复的超链接
            continue
        baike_id_list.append(link)
        quote = link.split('/')[2]    # 获取链接的标题
        link_title = urllib.parse.unquote(quote)
        href = title_href + link
        flag = True
        while True:
            try:
                req_text = requests.get(href, headers=request_headers).text
                break
            except:
                flag = False
                print("#####Please wait 10 seconds#####")
                time.sleep(10)
        if not flag:
            # 如果目标超链接页面无效的，则直接跳过该页面
            continue
        soup = BeautifulSoup(req_text, 'lxml')
        # 得到每个链接对应的义项描述
        link_label = get_link_label(soup)
        if link_label is None:
            continue
        item_dict['Link'] = href
        item_dict['Title'] = link_title
        item_dict['Label'] = link_label
        link_data.append(item_dict)

    return link_data



def get_link_label(soup):
    """获取到对应百度百科页面实体的义项描述，来作为出现多义词时的唯一标识"""
    if soup.find('div', attrs={'class':'lemma-summary'}) is None:
        # 说明出现错误页面，比如页面不存在等情况
        return None
    dd_label = soup.find('dd', attrs={'class':'lemmaWgt-lemmaTitle-title'})
    h_label = dd_label.find_all('h1')
    if len(h_label) == 2:
        # 说明title旁边存在括号，以此作为页面的义项描述
        h2_tag = h_label[1].get_text()
        h2_tag_strip = h2_tag.strip('（）')
        return h2_tag_strip
    elif soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'}):
        # 说明该页面仍然是一个多义词页面，但是页面布局的形式不同，采取不同的获取方法
        ul_label = soup.find('ul', attrs={'class': 'polysemantList-wrapper cmn-clearfix'})
        li_label = ul_label.find_all('li', attrs={'class': 'item'})
        for li in li_label:
            if li.find('span'):
                # <span>标签表示了该文本内容为当前页面，否则会是<a>标签
                text = li.get_text().strip('▪')
                return text
    else:
        # 对于非多义词界面，义项描述返回monoseme
        yixiang = 'monoseme'    # 以该标签标识该词在百度百科中为单义词
        return yixiang



def validate_href(href):
    """对标签内容进行检查，对于href内没有item项的href进行过滤"""
    item_compile = re.compile(r'^/item/.+') # 尾匹配，排除干扰的超链接
    if item_compile.search(href):
        return True
    else:
        return False

def page_type(soup):
    # 对于多义词列表的页面进行解析
    if soup.find('div', attrs={'class': 'lemmaWgt-subLemmaListTitle'}):
        return 1
    # 对于页面最上面有多义词title的页面进行解析
    elif soup.find('ul', attrs={'class':'polysemantList-wrapper cmn-clearfix'}):
        return 2
    else:
        # 对于不是多义词，只有每一个词的页面
        return 3

class CrawlerProcess(Process):
    def __init__(self, id_list, q, lock, id2subject, describe_dict, request_headers):
        """
        :param id_list: 包含实体id的实体池
        :param q: 每个进程保存爬取结果的队列
        :param lock: 进程锁
        :param id2subject: 实体id与实体名间的映射dict
        :param describe_dict: 包含义项描述（即实体消歧信息）的dict
        :param request_headers: requests访问请求头
        """
        Process.__init__(self)
        self.id_list = id_list
        self.q = q
        self.lock = lock
        self.id2subject = id2subject
        self.describe_dict = describe_dict
        self.request_headers = request_headers

    def run(self):
        # 每一个进程不断从实体池中取实体，直到实体池为空
        while len(self.id_list) != 0:
            # 加锁
            self.lock.acquire()
            if len(self.id_list) == 0:
                # 额外的一个退出判断，防止出现只有最后一个实体，但有多个进程进入了while循环的情况
                self.lock.release()
                break
            # 从实体池中随机选取一个实体
            choice_id = random.choice(self.id_list)
            # 选完后删除
            self.id_list.remove(choice_id)
            # 解锁
            self.lock.release()

            # 由实体id转换为对应的实体名
            subject = self.id2subject[choice_id]
            # 这里的义项描述，则表示额外的消歧信息，来帮助获取到正确的对应页面
            yixiang = self.describe_dict[choice_id]
            # 根据实体名，构造百度百科访问地址
            url = construct_url(keyword=subject)
            # 对于每个subject，获取符合其义项描述的对应页面下的所有超链接
            link_data = main_crawler(url, yixiang, self.request_headers)
            entity_link_dict = dict()
            if link_data is None or len(link_data) == 0:
                # 可能有页面没有超链接的情况
                entity_link_dict[choice_id] = "Null"
            else:
                entity_link_dict[choice_id] = link_data

            print(os.getpid(), choice_id, self.q.qsize(), entity_link_dict)

            # 将抓取到的数据放到队列中保存
            self.q.put(entity_link_dict)

            # 判断队列是否满队
            if self.q.full():
                # 释放队列内容，直到队列为空
                while not self.q.empty():
                    link_dict = self.q.get()
                    # 因为需要对本地文件进行写入，所以也需要加入锁，防止不同进程之间的写入混乱
                    self.lock.acquire()
                    with open('./multi_link_data/subject_hyperlinks.json', 'a', encoding='utf-8') as fin:
                        json.dump(link_dict, fin, ensure_ascii=False)
                        fin.write('\n')
                    self.lock.release()

if __name__ == '__main__':
    request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed- xchange;v=b3;q=0.9',
        'Accept - Encoding': 'gzip, deflate, br',
        'Accept - Language': 'zh-CN,zh;q=0.9',
        'Cache - Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'BAIDUID=003D94039A5FB16CE650EBCF5E72A45E:FG=1; BIDUPSID=003D94039A5FB16CE650EBCF5E72A45E; PSTM=1561885291; BDUSS=WVXUDRZSHdKeVJhaERyOXh2TTNoT3lPN3p4VE04SXhKSlVUWTd4S2JMMmtLMEZkSVFBQUFBJCQAAAAAAAAAAAEAAABh2fss1OfJz7XEtrm9rNPNzPUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKSeGV2knhldQ; H_PS_PSSID=; delPer=0; BD_CK_SAM=1; PSINO=2; BDRCVFR[BCzcNGRrF63]=mk3SLVN4HKm; BD_HOME=1; BD_UPN=12314753; BDORZ=FFFB88E999055A3F8A630C64834BD6D0; COOKIE_SESSION=2691823_0_9_0_66_16_0_2_9_5_115_2_3625418_0_2_0_1588747660_0_1588747658%7 C9%23191_140_1583480922%7C9; H_PS_645EC=b082T2nk%2FHreRxzRLh%2F4Lvy%2FrJ0eUckomxoWqhlovZkh4zkgCdpy%2FXI9AIKSPp5b9I1IZ3c; BDSVRTM=193',
        'Host': 'baike.baidu.com',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, like Gecko) Chrome/81.0.4044.129 Safari / 537.36'
    }

    id2subject_path = './data/id2subject.pkl'
    id2subject = pd.read_pickle(id2subject_path)

    yixiangmiaoshu_path = './data/describe_dict.pkl'
    describe_dict = pd.read_pickle(yixiangmiaoshu_path)
    # subject_list = list(id2subject.values())
    # test_subject = {'10026':'龙泉驿区', '10124':"JAZZ", '10051':'海市蜃楼'}

    start_time = time.time()
    id_list = Manager().list(id2subject.keys())
    process_num = 6
    q = Manager().Queue(100)
    lock = Lock()
    l = []
    for i in range(process_num):
        p = CrawlerProcess(id_list=id_list, q=q, lock=lock, id2subject=id2subject, describe_dict=describe_dict,
                           request_headers=request_headers)
        p.start()
        l.append(p)
    [p.join() for p in l]

    rest_result = [q.get() for j in range(q.qsize())]

    print("time: ", time.time() - start_time)



