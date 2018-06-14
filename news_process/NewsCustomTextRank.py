import datetime
import numpy as np

from sklearn.preprocessing import MinMaxScaler

from persistence.MongoManager import MongoManager

class NewsCustomTextRank(object):
    collection = dict()
    collection['words'] = 'news_words'
    collection['stop_words'] = 'news_stop_words'
    collection['summary'] = 'news_summary'

    # 처리주기
    period = 7

    def __init__(self, count_min=2, count_max=99, word_length_min=2):
        # 최소~최대 count 기준수
        self.count_min = count_min
        self.count_max = count_max

        # 최소 단어길이
        self.word_length_min = word_length_min

        self.documents = list()
        self.document_frequency = dict()    # document frequency of words
        self.idf = dict()

    @staticmethod
    def load_news_words(load_date, repository_manager=None):
        return repository_manager.find(NewsCustomTextRank.collection['words'], {'date': load_date})

    @staticmethod
    def load_stop_words(base_word, repository_manager=None):
        return repository_manager.find(NewsCustomTextRank.collection['stop_words'], {'base_word': base_word})

    def built_tfidf(self):
        # document_frequency 계산
        for document in self.documents:
            for key, value in zip(document['data_frequency']['keys'], document['data_frequency']['values']):
                if value > self.count_max:
                    continue
                elif value < self.count_min:
                    break
                elif len(str(key)) < self.word_length_min:
                    continue
                else:
                    if key not in self.document_frequency:
                        self.document_frequency[key] = 1
                    else:
                        self.document_frequency[key] = self.document_frequency[key] + 1

            # words_header 생성
            document['data_words_header'] = [word for word in document['data_frequency']['keys']
                                             & self.document_frequency.keys()]
            # print('data_words_header:', document['data_words_header'])
        else:
            print('[document_frequency process][complete]')

        # idf 계산
        for key in self.document_frequency.keys():
            self.idf[key] = np.log(len(self.documents) / (1 + self.document_frequency[key]))
        else:
            # print('Inverse Document Frequency:', self.idf)
            print('[idf process][complete]')

        # tf-idf 계산
        for document in self.documents:
            document['data_tfidf'] = list()
            for sentences in document['data_words']:
                sentences_tfidf = [0 for j in range(len(document['data_words_header']))]
                for word in sentences:
                    if word in document['data_words_header']:
                        sentences_tfidf[document['data_words_header'].index(word)] = self.idf[word] \
                              * document['data_frequency']['values'][document['data_frequency']['keys'].index(word)]
                else:
                    document['data_tfidf'].append(sentences_tfidf)

            # print(document['data_tfidf'])
        else:
            print('[tf-idf process][complete]')

    def built_graph(self):
        for document in self.documents:
            sentences = [set(sentences) & self.document_frequency.keys() for sentences in document['data_words']]
            # print('sentences:', sentences)

            document['data_graph'] = self.get_graph_matrix(lines=sentences, header=document['data_words_header'])
            # print(document['data_graph'])
        else:
            print('[data_graph process][complete]')

    def get_rank(self):
        for document in self.documents:
            document['data_rank'] = list()
            for weight, edge in zip(document['data_tfidf'], document['data_graph']):
                document['data_rank'].append(np.dot(weight, edge))
                # print('weight:', weight)
                # print('edge:', edge)
            # print(document['data_rank'])
        else:
            print('[get_rank process][complete]')

    def get_summary(self, summary_lines=1):
        for document in self.documents:
            # print(document['raw_data'])

            rank = [(index, key) for index, key in enumerate(document['data_rank'], 0)][1:] #header 제외
            summary = sorted(sorted(rank, key=lambda item: item[1], reverse=True)[:summary_lines]
                             , key=lambda item: item[0])
            # print(summary)

            #요약정보 save (0:header, 1~:summary)
            document['data_summary'] = list()
            document['data_summary'].append(document['raw_data'][0])
            for data in summary:
                document['data_summary'].append(document['raw_data'][data[0]])
            # else:
            #     print(document['data_summary'])
            #     if 'raw_label' in document:
            #         print('>>', document['raw_label'])
            #     print('*' * 50)
        else:
            print('[get_summary process][complete]')

    #calculate edge that linked word to word
    @staticmethod
    def get_graph_matrix(lines, header):
        # print('header:', header)
        graph = [[0 for j in range(len(header))] for i in range(len(lines))]
        # print('init graph:', graph)
        for row_num, rows in enumerate(lines, 0):
            for col_num, cols in enumerate(lines, 0):
                if row_num != col_num:
                    for word in rows:
                        # print('header.index(word):', header.index(word))
                        if (word in cols) and (word in header):
                            graph[row_num][header.index(word)] = graph[row_num][header.index(word)] + 1
        # print('calculate graph:', graph)
        return graph

def save_news_summary(s_date=None, e_date=None):
    print('*' * 50)
    print('[%s ~ %s] NewsTextRank' % (s_date, e_date))
    print('*' * 50)

    mongo = MongoManager()
    textRank = NewsCustomTextRank()

    # summary 할 news 조회
    day = s_date
    news_token = list()
    while day < e_date:
        news_token.extend(textRank.load_news_words(day.strftime('%Y%m%d'), repository_manager=mongo))
        day = day + datetime.timedelta(days=1)

    for token in news_token:
        # document내 date 정보 추가
        for article in token['article']:
            article['date'] = token['date']
        textRank.documents.extend(token['article'])

    textRank.built_tfidf()  # 단어-문서별 상관도분석
    textRank.built_graph()  # 문장별 연관도계산
    textRank.get_rank()     # 점수계산
    textRank.get_summary()  # 점수에 따른 data_summary 입력

    # 일자별 grouping
    summary_list = dict()
    for document in textRank.documents:
        if document['date'] not in summary_list:
            summary_list[document['date']] = list()

        data = dict()
        data['_id'] = document['_id']
        data['summary'] = document['data_summary']
        # print(data)
        summary_list[document['date']].append(data)

    # save
    for date in summary_list:
        con = {'date': date}

        if not mongo.find_one(NewsCustomTextRank.collection['summary'], con):
            # save
            obj = dict()
            obj['date'] = date
            obj['article'] = summary_list[date]
            print(obj)
            mongo.save_one(collection=NewsCustomTextRank.collection['summary'], data=obj)
        else:
            # load
            news_summary = mongo.find(NewsCustomTextRank.collection['summary'], con)
            for item in news_summary:
                print(item)

if __name__ == '__main__':

    s_date = datetime.date(2018, 5, 15)
    e_date = datetime.date(2018, 5, 20)

    print('*' * 50)
    print('[%s ~ %s] NewsCustomTextRank' % (s_date, e_date))
    print('*' * 50)

    while s_date < e_date:

        # rank 생성할 date 지정
        in_date = s_date
        out_date = s_date + datetime.timedelta(days=NewsCustomTextRank.period)
        save_news_summary(in_date, out_date)

        # week + 1
        s_date = out_date



