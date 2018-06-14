import datetime
import copy

from persistence.MongoManager import MongoManager
from news_scrap.News import News
from news_process.NewsCustomTextRank import NewsCustomTextRank

class NewsHAC(NewsCustomTextRank):
    collection = 'news_hac'
    period = NewsCustomTextRank.period

    def __init__(self, term_min_size=5):
        self.term_min_size = term_min_size
        self.terms_list = list()
        self.terms_index = None
        self.epoch = None

        NewsCustomTextRank.__init__(self)

    def append_terms_list(self, items, repository_manager=None):
        for item in items:
            terms = self.get_terms(item['data_frequency'])
            if terms:
                news = repository_manager.find_one(News.collection, {'_id': item['_id']})
                # print(news)

                data = dict()
                data['_id'] = item['_id']
                data['date'] = news['date']
                data['rank'] = news['rank']
                data['site'] = news['site']
                data['url'] = news['url']
                data['viewCnt'] = news['viewCnt'] if 'viewCnt' in news else -1
                data['terms'] = terms
                # print(data)

                self.terms_list.append(data)

    def get_terms(self, document):
        obj = set()
        for key, value in zip(document['keys'], document['values']):
            if value > self.count_max:
                continue
            elif value < self.count_min:
                break
            elif len(str(key)) < self.word_length_min:
                continue
            else:
                obj.add(key)

        if len(obj) < self.term_min_size:
            # print('get_terms_list size:', len(obj), obj)
            return None

        return obj

    def terms_onehot_encoding(self):
        obj = set()
        for data in self.terms_list:
            obj.update(data['terms'])
        self.terms_index = tuple(obj)

        for index, data in enumerate(self.terms_list, 0):
            obj = set()
            for term in data['terms']:
                obj.add(self.terms_index.index(term))
            self.terms_list[index]['terms'] = obj

    def merge_equal_terms_list(self):
        items = [[term] for term in self.terms_list]
        for t_index, term in enumerate(items, 0):
            for c_index, compare_term in enumerate(items, 0):
                if c_index != t_index and self.compare_to_terms(term[0], compare_term[0]):
                    term.extend(compare_term)
                    del items[c_index]

        # merge된 결과저장
        print('merge to list size: %d -> %d' % (len(self.terms_list), len(items)))
        self.terms_list = items

    def init_epoch(self):
        self.epoch = list()
        self.epoch.append({'terms': [term[0]['terms'] for term in self.terms_list],
                          'links': [{i} for i in range(len(self.terms_list))]})
        # print(self.epoch)

    def agglomerative_clustering(self):
        step = 0
        while len(self.epoch[-1]['links']) > 1:
            self.run_epoch(step)
            step = step + 1

    def run_epoch(self, step):
        # jaccard_metrix 계산
        jaccard_metrix = self.get_jaccard_coefficient(self.epoch[step]['terms'])

        # jaccard_metrix 최대값 구하기
        similarity = list()
        for rows in jaccard_metrix:
            similarity.append(max((cols, cols_num) for cols_num, cols in enumerate(rows)))

        (v, i, j) = max((rows[0], rows_num, rows[1]) for rows_num, rows in enumerate(similarity))

        # 이번 step 정보 입력
        self.epoch[step]['next_merge'] = {'from': j, 'to': i, 'max_similarity': v}

        print('step:%d, max_similarity: %f' % (step, v))

        # 다음 step 생성
        self.epoch.append({'links': self.epoch[step]['links'],
                           'terms': self.epoch[step]['terms'],
                           'next_merge': None
                           })

        # links 생성
        self.epoch[step + 1]['links'][i].update(self.epoch[step + 1]['links'][j])
        del self.epoch[step + 1]['links'][j]
        # print(self.epoch[step + 1]['links'])

        # terms 생성
        self.epoch[step + 1]['terms'][i].update(self.epoch[step + 1]['terms'][j])
        del self.epoch[step + 1]['terms'][j]
        # print(self.epoch[step + 1]['terms'])

    @staticmethod
    def compare_to_terms(a, b):
        return len(a['terms'] & b['terms']) == len(a['terms'] | b['terms'])

    @staticmethod
    def get_jaccard_coefficient(items):
        # items x items metrixs 생성
        obj = [[0 for cols in range(len(items))] for rows in range(len(items))]

        for i, rows in enumerate(items, 0):
            for j, cols in enumerate(items, 0):
                obj[i][j] = len(rows & cols) / len(rows | cols) if i != j else 0
        return obj

    @staticmethod
    def load_news_hac(s_date, e_date, repository_manager=None):
        return repository_manager.find_one(NewsHAC.collection, {'s_date': s_date, 'e_date': e_date})


def save_news_clustering(s_date=None, e_date=None):
    mongo = MongoManager()
    hac = NewsHAC()

    con = {'s_date': s_date.strftime('%Y%m%d'),
           'e_date': (e_date - datetime.timedelta(days=1)).strftime('%Y%m%d')}
    print('con:', con)

    if not mongo.find_one(NewsHAC.collection, con):
        # clustering 할 news 조회
        day = s_date
        while day < e_date:
            news_token = hac.load_news_words(day.strftime('%Y%m%d'), repository_manager=mongo)
            for token in news_token:
                hac.append_terms_list(items=token['article'], repository_manager=mongo)

            day = day + datetime.timedelta(days=1)

        hac.terms_onehot_encoding() # processiong to terms OneHotEncoding
        hac.merge_equal_terms_list()
        hac.init_epoch()
        hac.agglomerative_clustering()

        # save
        data = dict()
        data['s_date'] = con['s_date']
        data['e_date'] = con['e_date']
        data['terms_index'] = [item for item in hac.terms_index]    # tuple to list

        data['links_index'] = list()
        for articles in hac.terms_list:
            group_obj = list()
            for article in articles:
                obj = dict()
                obj['terms'] = [item for item in article['terms']]  # set to list
                obj['date'] = article['date']
                obj['site'] = article['site']
                obj['rank'] = article['rank']
                obj['url'] = article['url']
                obj['viewCnt'] = article['viewCnt']
                obj['_id'] = article['_id']
                group_obj.append(obj)
            data['links_index'].append(group_obj)

        data['epoch'] = list()
        mongo.save_one(collection=NewsHAC.collection, data=data)

        # 각 epoch push
        for epoch in hac.epoch:
            # obj = dict()
            # obj['next_merge'] = epoch['next_merge']
            # obj['links'] = [[item for item in link_set] for link_set in epoch['links']]
            # obj['terms'] = [[item for item in term_set] for term_set in epoch['terms']]

            # mongo.push(collection=NewsHAC.collection, con=con, data={'epoch': obj})
            mongo.push(collection=NewsHAC.collection, con=con, data={'epoch': epoch['next_merge']})

        print('s_date ~ e_date: %s ~ %s' % (data['s_date'], data['e_date']))
        # print('terms_index: ', data['terms_index'])
        # print('links_index', data['links_index'])
        # for step in data['epoch']:
        #     print(step)

    else:
        # load
        data = mongo.find_one(NewsHAC.collection, con)

        print('s_date ~ e_date: %s ~ %s' % (data['s_date'], data['e_date']))
        print('terms_index: ', data['terms_index'])
        print('links_index', data['links_index'])
        print('epoch length:', len(data['epoch']))
        # for step in data['epoch']:
        #     print(step)

if __name__ == '__main__':
    s_date = datetime.date(2018, 5, 20)
    e_date = datetime.date(2018, 5, 27)
    # e_date = s_date + datetime.timedelta(days=NewsCustomTextRank.period)

    while s_date.strftime('%Y%m%d') < e_date.strftime('%Y%m%d'):
        begin = e_date - datetime.timedelta(days=NewsCustomTextRank.period)
        end = e_date

        save_news_clustering(s_date=begin, e_date=end)

        # week + 1
        e_date = e_date - datetime.timedelta(days=NewsHAC.period)






