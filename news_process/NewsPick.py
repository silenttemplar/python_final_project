import datetime
import copy
import collections

import numpy

from persistence.MongoManager import MongoManager
from news_process.NewsHAC import NewsHAC
from news_process.NewsCustomTextRank import NewsCustomTextRank
from news_scrap.News import News

class NewsPick(NewsHAC):
    collection = 'news_pick'

    def __init__(self, pick_cnt=5, std_per=0.01):
        self.pick_cnt = pick_cnt
        self.std_per = std_per

        self.base_pick_news = None

        NewsHAC.__init__(self)

    def load_hac(self, s_date, e_date, repository_manager=None):
        document = self.load_news_hac(s_date=s_date, e_date=e_date, repository_manager=repository_manager)

        if document:
            self.terms_index = document['terms_index']
            self.terms_list = document['links_index']
            self.sort_news(self.terms_list) # merge된 기사 내 우선순위별 sorting
            for links in self.terms_list:
                link = links[0]
                link['terms'] = set(link['terms'])
                # print(link['terms'])

            self.epoch = [{'next_merge': next_merge} for next_merge in document['epoch']]
            self.epoch[0]['links'] = [{i} for i in range(len(self.terms_list))]
            for epoch in self.epoch[1:]:
                epoch['links'] = self.epoch[0]['links']

        else:
            raise Exception('document is not found. data is not prepared!')

        self.base_pick_news = None
        self.pick_news = None

    def agglomerative_clustering(self):
        self.min_group_cnt = self.pick_cnt
        self.min_member_cnt = int(len(self.terms_list) * self.std_per)
        print('min_group_cnt: %d, min_member_cnt: %d' % (self.min_group_cnt, self.min_member_cnt))

        step = 0
        while self.epoch[step]['next_merge']:
            self.run_epoch(step)
            # print(self.epoch[step]['next_merge'])
            # print('links length:', len(self.epoch[step]['links']))

            unique, counts = numpy.unique(numpy.array([len(links) for links in self.epoch[step]['links']]),
                                          return_counts=True)
            chk_pick_news_trend = dict(zip(unique, counts))
            # print(chk_pick_news_trend)
            if sum(chk_pick_news_trend[member_cnt] for member_cnt in chk_pick_news_trend.keys()
                   if member_cnt >= self.min_member_cnt) >= self.min_group_cnt \
                    and (not self.base_pick_news):
                self.set_base_pick_news(step)

            step = step + 1

    def run_epoch(self, step):
        j = self.epoch[step]['next_merge']['from']
        i = self.epoch[step]['next_merge']['to']
        v = self.epoch[step]['next_merge']['max_similarity']

        print('step:%d, max_similarity: %f' % (step, v))

        # links 생성
        self.epoch[step + 1]['links'][i].update(self.epoch[step + 1]['links'][j])
        del self.epoch[step + 1]['links'][j]
        # print(self.epoch[step + 1]['links'])

    def set_base_pick_news(self, step):
        self.base_pick_news = [link for link in self.epoch[step]['links'] if len(link) >= self.min_member_cnt]
        print(step, 'base_pick_news:', self.base_pick_news)

    def get_pick_news(self, repository_manager=None):
        pick_news = list()
        for base in self.base_pick_news:
            pick_news.append([self.terms_list[num][0] for num in base])
        pick_news = sorted(pick_news, key=lambda item: len(item), reverse=True)

        self.sort_news(pick_news)   #기사 내 우선순위별 sorting
        for index, news in enumerate(pick_news, 0):
            for new in news:
                article = repository_manager.find_one(News.collection, {'_id': new['_id']})
                print(index, article['article']['title'])
        pick_news = [rank[0] for rank in pick_news]

        for news in pick_news:
            # print('date:', news['date'])
            summary = repository_manager.find_one(NewsCustomTextRank.collection['summary'], {'date': news['date']})
            for data in summary['article']:
                if data['_id'] == news['_id']:
                    news['header'] = data['summary'][0]
                    news['summary'] = data['summary'][1:]
                else:
                    continue
        self.pick_news = pick_news

    def sort_news(self, items):
        for group in items:
            group = sorted(group, key=lambda item: int(item['date']), reverse=True)
            group = sorted(group, key=lambda item: int(item['rank']))
            group = sorted(group, key=lambda item: item['site'])
            group = sorted(group, key=lambda item: int(str(item['viewCnt']).replace(',', '')), reverse=True)

def save_news_pick(s_date, e_date):
    mongo = MongoManager()
    pick = NewsPick()

    print('*' * 50)
    print('[%s ~ %s] save_news_pick' % (s_date, e_date))
    print('*' * 50)

    con = {'s_date':s_date.strftime('%Y%m%d'), 'e_date':e_date.strftime('%Y%m%d')}
    if not mongo.find_one(NewsPick.collection, con):
        pick.load_hac(s_date=con['s_date'], e_date=con['e_date'], repository_manager=mongo)
        pick.agglomerative_clustering()
        pick.get_pick_news(repository_manager=mongo)

        data = dict()
        data['s_date'] = con['s_date']
        data['e_date'] = con['e_date']
        data['article'] = list()
        for news in pick.pick_news:
            obj = dict()
            obj['_id'] = news['_id']
            obj['header'] = news['header']
            obj['summary'] = news['summary']
            obj['url'] = news['url']

            data['article'].append(obj)

        mongo.save_one(collection=NewsPick.collection, data=data)
    else:
        # load
        data = mongo.find_one(NewsPick.collection, con)

        print('s_date ~ e_date: %s ~ %s' % (data['s_date'], data['e_date']))
        print('article:', data['article'])

if __name__ == "__main__":

    s_date = datetime.date(2018, 1, 1)
    e_date = datetime.date(2018, 5, 1)

    print('*' * 50)
    print('[%s ~ %s] NewsPick' % (s_date, e_date))
    print('*' * 50)

    while s_date.strftime('%Y%m%d') < e_date.strftime('%Y%m%d'):
        begin = s_date
        end = s_date + datetime.timedelta(days=NewsCustomTextRank.period - 1)

        # begin = e_date - datetime.timedelta(days=NewsCustomTextRank.period)
        # end = e_date

        save_news_pick(s_date=begin, e_date=end)

        # week + 1
        # e_date = e_date - datetime.timedelta(days=NewsHAC.period)
        s_date = s_date + datetime.timedelta(days=NewsCustomTextRank.period)




