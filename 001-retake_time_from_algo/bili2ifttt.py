import datetime
import json
import re
import time
import traceback

import feedparser
import requests as R
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()
c = "mysql+pymysql://USERNAME:PASSWORD@IP:PORT/DB?charset=utf8mb4"
engine = create_engine(c, max_overflow=500)


class Fetcher(Base):
    # 表名
    __tablename__ = 'fetcher'
    # 表字段
    id = Column(Integer, primary_key=True)  # 主键、默认自增
    fetch_method = Column(String(32), index=True)
    fetch_url = Column(String(256), index=True)
    webhook_method = Column(String(32), index=True)
    webhook_url = Column(String(256), index=True)
    flag = Column(String(256), index=False, default='')
    last_run = Column(DateTime(), default=datetime.datetime.now)
    white_re = Column(String(256), index=False, default='')
    black_re = Column(String(256), index=False, default='')

    __table_args__ = (UniqueConstraint('fetch_method', 'fetch_url', 'webhook_url', 'webhook_method', name='fethcer_u'),)  # 唯一索引


class BiliVideo(Base):
    # 表名
    __tablename__ = 'bilivideo'
    # 表字段
    id = Column(Integer, primary_key=True)  # 主键、默认自增
    fetcher_id = Column(Integer, index=True)
    uid = Column(Integer, index=True)
    uname = Column(String(32), index=True)
    title = Column(String(256), index=False)
    publish_time = Column(DateTime, index=False, default=datetime.datetime.fromtimestamp(int(time.time())))
    video_link = Column(String(256), index=True)
    status = Column(String(32), index=True)

    __table_args__ = (UniqueConstraint('fetcher_id', 'uid', 'video_link', name='bilivideo_u'),)  # 唯一索引


def init_db():
    Base.metadata.create_all(engine)


init_db()


session = sessionmaker(bind=engine)  # 指定引擎


def ifttt_api(webhook, uname, publish_time, title, url):
    headers = {
        'Content-Type': 'application/json',
    }
    t = publish_time.strftime('%m%d.%H%M')
    data = json.dumps({"value1": f'【{uname}】({t})', "value2": title, "value3": url})
    try:
        rsp = R.post(webhook, headers=headers, data=data, timeout=10)
        return 'Congratulations' in rsp.text
    except:
        return False


def get_videos(feed):
    xml = R.get(feed).text
    xml = feedparser.parse(xml)

    data = []
    for one in xml.entries:
        data.append(
            {
                'uid': int(feed.split('/')[-1]),
                'uname': one.get('author', '?'),
                'url': one['link'],
                'time': datetime.datetime.strptime(one['published'], '%a, %d %b %Y %H:%M:%S %Z'),
                'title': one['title'],
            }
        )
    return data


def check_db(f, v):
    # f是fetch对象
    if f.white_re:
        if not re.match(f.white_re, v.get('title')):
            print(v.get('title'), '不符合白名单正则', f.white_re)
            return True
    if f.black_re:
        if re.match(f.black_re, v.get('title')):
            print(v.get('title'), '符合黑名单正则', f.black_re)
            return True
    D1 = session()
    item = (
        D1.query(BiliVideo)
        .filter(BiliVideo.uid == v.get('uid'))
        .filter(BiliVideo.fetcher_id == f.id)
        .filter(BiliVideo.video_link == v.get('url'))
        .first()
    )
    if item is None:
        D2 = session()
        try:
            print(v)
            print('-----------------------')
            item = BiliVideo(
                fetcher_id=f.id,
                uid=v.get('uid'),
                uname=v.get('uname'),
                publish_time=v.get('time'),
                video_link=v.get('url'),
                title=v.get('title'),
                status='new',
            )

            D2.add(item)
            D2.commit()
            print('插入成功')
        except Exception as e:
            print(e)
            D2.rollback()
        finally:
            D2.close()
    D1.close()


def push_notify():
    D3 = session()
    videos = D3.query(BiliVideo).filter(BiliVideo.status == 'new').limit(3).all()
    print(f'一共有{len(videos)}个待推送')
    for v in videos:
        # print(D3.query(Fetcher).filter(Fetcher.id == v.fetcher_id))
        f = D3.query(Fetcher).filter(Fetcher.id == v.fetcher_id).first()
        if f.webhook_method == 'ifttt':
            url = v.video_link.replace('https://www.bilibili.com/video/', 'bilibili://video/').replace('av', '')
            status = ifttt_api(webhook=f.webhook_url, uname=v.uname, publish_time=v.publish_time, title=v.title, url=url)
            if status:
                v.status = 'pushed'
                D3.commit()
                print('推送成功')
            else:
                print('推送失败')
    D3.close()


def main(*args, **kwargs):
    # 获取fetch_url
    # 获取更新
    # 判定是否插入数据库， select or insert
    # 获取status 为待推送
    # 推送，成功后修改status

    # 获取fetch_url, method==rsshub
    D = session()
    fetcher = D.query(Fetcher).filter(Fetcher.fetch_method == 'rsshub').order_by(Fetcher.last_run).limit(5).all()
    print(f'一共有{len(fetcher)}个fetcher待检查')
    print([f.flag for f in fetcher])
    for f in fetcher:
        url = f.fetch_url
        try:
            videos = get_videos(url)
        except Exception as e:
            traceback.print_exc()
            continue
        print(f'{url} {f.flag} RSS返回了{len(videos)}个结果')
        for v in videos:
            try:
                check_db(f, v)
            except Exception as e:
                print(e)
                continue
        f.last_run = datetime.datetime.now() + datetime.timedelta(hours=8)
        D.commit()
    D.close()
    push_notify()


if __name__ == '__main__':
    main()
