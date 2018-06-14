from bs4 import BeautifulSoup
from time import sleep
from urllib.error import HTTPError

from news_scrap.News import News
from persistence.MongoManager import MongoManager

class DaumNews(News):
    site = News.DAUM
    baseUrl = 'http://media.daum.net/ranking/popular'

    def __init__(self, date, database='repo', collection=News.collection):
        self.date = date
        self.regDate = date
        self.database = database
        self.collection = collection

    def getArticleLinks(self):
        url = self.createUrl(
                {'include': 'society,politics,culture,economic,foreign,digital', 'regDate': self.regDate})
        print('url:', url)

        links = self.parseNewsLinks(self.getHTML(url))
        # print(links)
        return links

    def parseNewsLinks(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        links = []
        for link in soup.select("div#mArticle > div.rank_news > ul.list_news2 > li"):
            rank = link.select_one('span.screen_out').string
            href = link.find('a')['href']
            links.append(href)
            # print(rank, '>', href)
        return links

    def getArticle (self, html):
        obj = dict()

        soup = BeautifulSoup(html, 'html.parser')
        article = soup.select_one('div#kakaoContent')

        #제목
        header = article.select_one('div.head_view > .tit_view').text.strip()
        print(self.date, 'header:', header)
        obj['title'] = header

        #요약
        summary = []
        for line in article.select('div.head_view > div.util_view p'):
            s = line.text.strip()
            # print('summary line:', s)
            if s:
                summary.append(s)
        else:
            # print('summary:', summary)
            if summary:
                obj['summary'] = summary

        #본문
        body = article.select('div#cMain > div#mArticle > div.news_view > div.article_view > section > div, p')
        body_arr = []
        for line in body:
            s = line.text.strip()
            # print('body line:', s)
            if s:
                body_arr.append(s)
        else:
            obj['contents'] = body_arr

        return obj

    def getArticles(self, links):
        articles = []
        for idx, link in enumerate(links, 1):
            obj = dict()
            obj['url'] = link
            obj['rank'] = idx

            try:
                articleHTML = self.getHTML(link)
                article = self.getArticle(articleHTML)
                obj['article'] = article
                articles.append(obj)
                # print('getArticleSummary summary:', summary)
            except HTTPError as e:
                print('getArticles HTTPError', obj['rank'], e)
            except:
                print('getArticles Error', obj['rank'])

            sleep(0.2)
        return articles

    def save(self, items, manager):
        # 공통사항 추가
        obj = dict()
        obj['site'] = self.site
        obj['date'] = self.date

        for item in items:
            item.update(obj)
            manager.save_one(self.collection, item)

if __name__ == '__main__':
    daumNews = DaumNews('20171012')
    repo = MongoManager(database=daumNews.database)
    con = {'site': daumNews.site, 'date': daumNews.date}

    if not repo.find_one(daumNews.collection, con):
        links = daumNews.getArticleLinks()
        articles = daumNews.getArticles(links)
        # print(articles)
        print('links len:', len(links), 'articles len:', len(articles))

        daumNews.save(articles, repo)
    else:
        for article in repo.find(daumNews.collection, con):
            print(article)







