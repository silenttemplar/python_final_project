import datetime
import time

from konlpy.tag import Kkma
from konlpy.tag import Twitter

from news_scrap.DaumNews import DaumNews
from news_scrap.NaverNews import NaverNews
from persistence.MongoManager import MongoManager

class NewsTokenization(object):
    collection = 'news_words'

    def __init__(self, nouns_tools=None):
        self.nouns_tools = nouns_tools

    #sentence to words by set
    def get_words(self, sentences):
        words = set()
        for tool in self.nouns_tools:
            words.update(tool.nouns(sentences))
        return words

    # calculate word frequency
    def get_term_frequency(self, sentences):
        frequency = dict()
        frequency['keys'] = []
        frequency['values'] = []

        for sentence in sentences:
            for word in sentence:
                if word not in frequency['keys']:
                    frequency['keys'].append(word)
                    frequency['values'].append(1)
                else:
                    index = frequency['keys'].index(word)
                    frequency['values'][index] = frequency['values'][index] + 1
        # print('frequency:', frequency)

        sorted_frequency = sorted(zip(frequency['values'], frequency['keys']), key=lambda x: x[0], reverse=True)
        frequency['keys'] = [key[1] for key in sorted_frequency]
        frequency['values'] = [key[0] for key in sorted_frequency]
        # print('sorted_frequency:', frequency)
        return frequency

    @staticmethod
    def load_news(load_date, repository_manager=None, news_list=None):
        items = list()
        for news in news_list:
            news.date = load_date
            items.extend(
                repository_manager.find(news.collection, {'site': news.site, 'date': news.date}))
        return items

# raw data(news) token 처리 후 저장
def save_news_words(s_date=None, e_date=None):
    nouns_tools = [Kkma(), Twitter()]  # 분석기
    token = NewsTokenization(nouns_tools=nouns_tools)

    mongo = MongoManager()
    news = [DaumNews(s_date.strftime('%Y%m%d')), NaverNews(s_date.strftime('%Y%m%d'))]  # load할 news 목록

    while s_date.strftime('%Y%m%d') < e_date.strftime('%Y%m%d'):
        day = s_date.strftime('%Y%m%d')
        con = {'date': day}

        if not mongo.find_one(NewsTokenization.collection, con):
            articles = token.load_news(day, repository_manager=mongo, news_list=news)
            print('[NewsTokenization][day: %s][article len: %d]' % (day, len(articles)))
            # articles = articles[2:3]

            datas = list()
            for article in articles:
                data = dict()
                data['_id'] = article['_id']

                # title(0) + contents(1~)
                lines = list()
                lines.append(article['article']['title'])
                lines.extend(article['article']['contents'])

                data['raw_data'] = lines
                data['data_words'] = [[item for item in token.get_words(line)] for line in data['raw_data']]
                data['data_frequency'] = token.get_term_frequency(data['data_words'])

                if 'summary' in article['article']:
                    data['raw_label'] = article['article']['summary']
                    data['label_words'] = [[item for item in token.get_words(line)] for line in data['raw_label']]
                    data['label_frequency'] = token.get_term_frequency(data['label_words'])

                print(data)
                datas.append(data)
            else:
                #save
                obj = dict()
                obj['date'] = day
                obj['article'] = datas
                mongo.save_one(collection=NewsTokenization.collection, data=obj)
        else:
            news_words_list = mongo.find(NewsTokenization.collection, con)
            for news_words in news_words_list:
                print('news_words len:', len(news_words))

        # day + 1
        s_date = s_date + datetime.timedelta(days=1)

if __name__ == '__main__':

    # 시작일(yyyy, mm, dd) ~ 종료일(yyyy, mm, dd)
    s_date = datetime.date(2018, 5, 15)
    e_date = datetime.date(2018, 5, 20)
    print('*' * 50)
    print('[%s ~ %s] News Tokenization' % (s_date, e_date))
    print('*' * 50)

    save_news_words(s_date=s_date, e_date=e_date)










