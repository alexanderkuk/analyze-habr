#!/usr/bin/env python
# encoding: utf8

import re
import os
import os.path
from hashlib import sha1
from collections import namedtuple
from math import ceil
import datetime

import requests
requests.packages.urllib3.disable_warnings()

import pandas as pd
from bs4 import BeautifulSoup

import seaborn as sns
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib import rc
# For cyrillic labels
rc('font', family='Verdana', weight='normal')


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'
}
DATA_DIR = 'data'
HTML_DIR = os.path.join(DATA_DIR, 'html')
HTML_LIST = os.path.join(HTML_DIR, 'list.txt')
DATA_DIR = 'data'
HUBS_CATALOG_PAGES = 7
HUB_PAGE_SIZE = 10
MONTHS = [
    u'января',
    u'февраля',
    u'марта',
    u'апреля',
    u'мая',
    u'июня',
    u'июля',
    u'августа',
    u'сентября',
    u'октября',
    u'ноября',
    u'декабря',
]
CURRENT_YEAR = 2016
POSTS = os.path.join(DATA_DIR, 'posts.csv')
SIMILARWEB_KEY = 'xxx'
SIMILARWEB_HABR = os.path.join(DATA_DIR, 'similarweb_habr.xlsx')
BLUE = '#4a71b2'


HubsRecord = namedtuple('HubsRecord', ['id', 'name', 'subscribers', 'posts'])
Votes = namedtuple('Votes', ['up', 'down', 'value'])
PostRecord = namedtuple(
    'PostRecord',
    ['url', 'title', 'date', 'hubs', 'company',
     'votes', 'views', 'stars', 'user', 'comments']
)


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def hash_url(url):
    return sha1(url.encode('utf8')).hexdigest()


def get_html_filename(url):
    return '{hash}.html'.format(
        hash=hash_url(url)
    )


def get_html_path(url):
    return os.path.join(
        HTML_DIR,
        get_html_filename(url)
    )


def list_html_cache():
    with open(HTML_LIST) as file:
        for line in file:
            line = line.decode('utf8').strip()
            hash, url = line.split('\t', 1)
            yield url


def update_html_cache(url):
    with open(HTML_LIST, 'a') as file:
        hash = hash_url(url)
        file.write('{hash}\t{url}\n'.format(
            hash=hash,
            url=url.encode('utf8')
        ))


def dump_html(url, html):
    path = get_html_path(url)
    if html is None:
        html = ''
    with open(path, 'w') as file:
        file.write(html)
    update_html_cache(url)


def load_html(url):
    path = get_html_path(url)
    with open(path) as file:
        return file.read()


def download_url(url):
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=5
        )
        return response.content
    except requests.RequestException:
        return None


def fetch_url(url):
    html = download_url(url)
    dump_html(url, html)


def fetch_urls(urls):
    for url in urls:
        fetch_url(url)


def get_hubs_url(page):
    return 'https://habrahabr.ru/hubs/page{page}/'.format(
        page=page
    )


def get_soup(html):
    return BeautifulSoup(html, 'lxml')


def parse_habr_int(value):
    if value.isdigit():
        return int(value)
    else:
        value, scale = value[:-1], value[-1]
        assert scale == 'k', scale
        if ',' in value:
            thousands, _, hundreds = value.partition(',')
        else:
            thousands = value
            hundreds = 0
        return int(thousands) * 1000 + int(hundreds) * 100


def parse_hubs_page(html):
    soup = get_soup(html)
    for item in soup.find_all('div', class_='hub'):
        link = item.find('div', class_='title').find('a')
        title = link.text
        match = re.match(r'^https://habrahabr.ru/hub/([^/]+)/$', link['href'])
        id = match.group(1)
        stats = item.find('div', class_='stat')
        subscribers, posts = stats.find_all('a')
        match = re.match(ur'^(.+) подписчик(|а|ов)$', subscribers.text)
        subscribers = parse_habr_int(match.group(1))
        match = re.match(ur'^(.+) публикаци(я|и|й)$', posts.text)
        posts = parse_habr_int(match.group(1))
        yield HubsRecord(id, title, subscribers, posts)    


def list_hubs():
    for page in xrange(1, HUBS_CATALOG_PAGES + 1):
        url = get_hubs_url(page)
        html = load_html(url)
        for record in parse_hubs_page(html):
            yield record


def get_pages_count(total, size):
    return int(ceil(float(total) / size))


def get_hub_url(id, page):
    return 'https://habrahabr.ru/hub/{id}/all/page{page}/'.format(
        id=id,
        page=page
    )


def list_hub_urls(hubs):
    for record in sorted(hubs, key=lambda _: _.posts):
        pages = get_pages_count(record.posts, HUB_PAGE_SIZE)
        for page in xrange(1, pages + 1):
            yield get_hub_url(record.id, page)


def parse_post_date(value):
    match = re.search(r'(\d+) (\w+) (\d{4})?', value, re.U)
    if match:
        day, month, year = match.groups()
        day = int(day)
        month = MONTHS.index(month) + 1
        year = int(year or CURRENT_YEAR)
        return datetime.date(year, month, day)


def parse_hubs(soup):
    hubs = []
    company = None
    for item in soup.find_all('a'):
        url = item['href']
        match = re.match(r'^https://habrahabr.ru/hub/([^/]+)/$', url)
        if match:
            hubs.append(match.group(1))
        else:
            match = re.match(r'^https://habrahabr.ru/company/([^/]+)/$', url)
            # there can several companies
            # https://habrahabr.ru/company/google/blog/268251/
            # assert company is None 
            company = match.group(1)
    return hubs, company


def parse_votes(item):
    up, down = 0, 0
    if item:
        title = item['title']
        match = re.match(ur'Общий рейтинг \d+: ↑([\d.-]+) и ↓([-\d.-]+)', title)
        up, down = match.groups()
        # up and down can be float. do not know why
        # https://habrahabr.ru/post/37595/
        up = int(float(up))
        down = int(float(down))
    return Votes(up, down, up - down)


def parse_posts(html):
    soup = get_soup(html)
    for item in soup.find_all('div', class_='post'):
        if item.find('div', class_='megapost-cover'):
            continue
        date = parse_post_date(item.find('div', class_='published').text)
        link = item.find('a', class_='post_title')
        url = link['href']
        title = link.text
        hubs, company = parse_hubs(item.find('div', class_='hubs'))
        info = item.find('ul', class_='postinfo-panel')
        votes = parse_votes(
            info.find('span', class_='voting-wjt__counter-score')
        )
        views = parse_habr_int(info.find('div', class_='views-count_post').text)
        stars = parse_habr_int(info.find('span', class_='favorite-wjt__counter').text)
        user = info.find('a', class_='post-author__link')
        if user:
            # user is rssbot
            # https://habrahabr.ru/company/yandex/blog/188314/
            user = user.text.strip()
        comments = info.find('a', class_='post-comments__link_all')
        if comments:
            comments = int(comments.text)
        else:
            comments = 0
        yield PostRecord(
            url, title, date, hubs, company,
            votes, views, stars, user, comments
        )


def load_raw_posts(urls):
    for url in urls:
        html = load_html(url)
        for record in parse_posts(html):
            yield record


def dump_posts(posts):
    data = []
    for post in posts:
        (url, title, date, hubs, company,
         votes, views, stars, user, comments) = post
        hubs = ', '.join(hubs)
        up, down, _ = votes
        data.append((
            url, title, date, hubs, company,
            up, down, views, stars, user, comments
        ))
    table = pd.DataFrame(
        data,
        columns=[
            'url', 'title', 'date', 'hubs', 'company',
            'up', 'down', 'views', 'stars', 'user', 'comments'
        ]
    )
    table.to_csv(POSTS, index=False, encoding='utf8')


def parse_date(date):
    return datetime.datetime.strptime(date, '%Y-%m-%d')


def load_posts():
    table = pd.read_csv(POSTS)
    table = table.where(pd.notnull(table), None)
    for index, row in table.iterrows():
        (url, title, date, hubs, company,
         up, down, views, stars, user, comments) = row
        if date:
            date = parse_date(date)
        hubs = hubs.split(', ')
        votes = Votes(up, down, up - down)
        yield PostRecord(
            url, title, date, hubs, company,
            votes, views, stars, user, comments
        )


def unique_posts(posts):
    urls = set()
    for post in posts:
        url = post.url
        if url not in urls:
            yield post
        urls.add(url)


def download_similarweb_visits(domain, start='1-2015', stop='12-2015'):
    response = requests.get(
        'http://api.similarweb.com/Site/{domain}/v1/visits'.format(
            domain=domain
        ),
        params={
            'gr': 'weekly',
            'start': start,
            'end': stop,
            'Format': 'JSON',
            'UserKey': SIMILARWEB_KEY
        }
    )
    return response.json()


def download_habr_similarweb_visits():
    series = {}
    for year in xrange(2012, 2016):
        data = download_similarweb_visits(
            'habrahabr.ru',
            start='1-{year}'.format(year=year),
            stop='12-{year}'.format(year=year)
        )
        for record in data['Values']:
            date = parse_date(record['Date'])
            value = record['Value']
            series[date] = value
    return series
