import datetime
from time import sleep
import time

from news_scrap.DaumNews import DaumNews
from news_scrap.NaverNews import NaverNews
from persistence.MongoManager import MongoManager

'''
    naver, daum news 가져오기
'''
def get_news(s_date=None, e_date=None):
    startTime = time.time()
    mongo = MongoManager()

    while  s_date.strftime('%Y%m%d') < e_date.strftime('%Y%m%d'):
        # 크롤링 날짜 추출
        day = s_date.strftime('%Y%m%d')
        print('current yyyymmdd:', day)

        news = [DaumNews(day), NaverNews(day)]

        for new in news:
            con = {'site': new.site, 'date': new.date}
            if not mongo.find_one(new.collection, con):
                links = new.getArticleLinks()
                articles = new.getArticles(links)
                print('links len: %d, articles len: %d' % (len(links), len(articles)))

                new.save(articles, mongo)
                sleep(1)
            else:
                articles = mongo.find(new.collection, con)
                for article in articles:
                    print(article)

        # '''
        #     daum 뉴스 크롤링
        # '''
        # daumNews = DaumNews(day)
        # con = {'site': daumNews.site, 'date': daumNews.date}
        #
        # if not mongo.find_one(daumNews.collection, con):
        #     links = daumNews.getArticleLinks()
        #     articles = daumNews.getArticles(links)
        #     print('links len: %d, articles len: %d' % (len(links), len(articles)))
        #
        #     daumNews.save(articles, mongo)
        #     sleep(1)
        # else:
        #     articles = mongo.find(daumNews.collection, con)
        #     for article in articles:
        #         print(article)
        #
        # '''
        #     naver 뉴스 크롤링
        # '''
        # nhnNews = NaverNews(day)
        # con = {'site': nhnNews.site, 'date': nhnNews.date}
        #
        # if not mongo.find_one(nhnNews.collection, con):
        #     links = nhnNews.getArticleLinks()
        #     # print(links)
        #     articles = nhnNews.getArticles(links)
        #     print('links len: %d, articles len: %d' % (len(links), len(articles)))
        #
        #     nhnNews.save(articles, mongo)
        #     sleep(1)
        # else:
        #     articles = mongo.find(nhnNews.collection, con)
        #     for article in articles:
        #         print(article)

        # day + 1
        s_date = s_date + datetime.timedelta(days=1)

    else:
        print('*' * 60)
        print('[%s ~ %s] news scrap complete' % (s_date.strftime('%Y%m%d'), e_date.strftime('%Y%m%d')))
        print('*' * 60)

    labsTime = time.time() - startTime
    print('실행된 시간=', labsTime, 'sec')
    if labsTime / 60 > 1:
        print(labsTime / 60, 'min')

if __name__ == '__main__':
    # 시작일(yyyy, mm, dd) ~ 종료일(yyyy, mm, dd)
    s_date = datetime.date(2018, 5, 20)
    e_date = datetime.date(2018, 5, 27)
    print('s_date:', s_date, 'e_date:', e_date)

    get_news(s_date=s_date, e_date=e_date)

