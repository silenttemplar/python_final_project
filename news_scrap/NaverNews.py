import urllib.request as req
import urllib.parse as parse
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import json
from time import sleep

from news_scrap.News import News
from persistence.MongoManager import MongoManager

class NaverNews(News):
    site = News.NAVER
    siteUrl = 'http://news.naver.com'
    baseUrl = siteUrl+'/main/ranking/popularDay.nhn'

    summaryUrl = 'http://tts.news.naver.com/article/{oid}/{aid}/summary'

    def __init__(self, date, database='repo', collection=News.collection):
        self.date = date
        self.database = database
        self.collection = collection

    def getArticleLinks(self):
        # 정치, 경제, 사회, 세계
        sectionIds = [100, 101, 102, 104]

        urls = [self.createUrl({
            'rankingType': 'popular_day', 'sectionId': sectionId, 'date': self.date}
        ) for sectionId in sectionIds]
        print('urls:', urls)

        links = []
        for url in urls:
            html = self.getHTML(url, charset='euc-kr')
            links.extend(self.parseNewsLinks(html))

        return links

    def parseNewsLinks(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        for idx, link in enumerate(soup.select("ol.ranking_list > li"), 1):
            obj = dict()
            obj['href'] = self.siteUrl + link.select_one('div.ranking_headline > a')['href']

            viewCnt = link.select_one('div.ranking_view')
            if viewCnt:
                obj['viewCnt'] = viewCnt.text.strip()
            else:
                obj['viewCnt'] = -1

            # print(obj)
            links.append(obj)
        return links

    def getArticle(self, html):
        obj = dict()

        soup = BeautifulSoup(html, 'html.parser')
        article = soup.select_one('div#main_content')
        # print('article:', article)

        #제목
        header = article.select_one('div.article_header > div.article_info > #articleTitle').text.strip()
        print(self.date, 'header:', header)
        obj['title'] = header

        #원문link
        orgin_link = article.select_one('div.article_header > div.article_info > div.sponsor > a.btn_artialoriginal')
        if orgin_link:
            obj['orgin_link'] = orgin_link['href']
            # print(self.date, 'orgin_link:', obj['orgin_link'])

        #본문
        body_arr = []

        #내부 script 제거
        body = article.select_one('div.article_body > #articleBodyContents')
        body.find('script').extract()
        lines = body.getText(separator="\n").strip()
        # print('type(lines):', type(lines), ', body: ', lines)
        for line in lines.split('\n'):
            s = line.strip()
            # print('body line:', s)
            if s:
                body_arr.append(s)
        else:
            obj['contents'] = body_arr
        # print(self.date, 'contents:', body_arr)

        return obj

    def getArticleSummary(self, oid, aid):
        obj = dict()

        '''
            기사 요약 조회 url 
            - format
            http://tts.news.naver.com/article/{oid}/{aid}/summary
            ex) oid=055, aid=0000636276 인 경우.
            http://tts.news.naver.com/article/055/0000636276/summary
        '''
        url = self.summaryUrl.format(oid=oid, aid=aid)
        # print('getArticleSummary url:', url)

        try:
            response = json.loads(self.getHTML(url))
            # print('getArticleSummary response:', response, 'type: ', type(response))

            summary = []
            lines = response['summary']
            if lines:
                lines = lines.split('<br/><br/>')
                for line in lines:
                    summary.append(line)
                else:
                    obj['summary'] = summary
            # print('getArticleSummary summary:', summary)
        except HTTPError as e:
            print('getArticleSummary', e)
        except:
            print('getArticleSummary Error')

        return obj

    def getArticles(self, links):
        articles = []
        for idx, info in enumerate(links, 1):
            url = info['href']
            params = parse.parse_qs(parse.urlparse(url).query)

            obj = dict()
            obj['url'] = url
            obj['viewCnt'] = info['viewCnt']
            obj['rankingSectionId'] = params['rankingSectionId'][0]
            obj['rank'] = params['rankingSeq'][0]

            try:
                articleHTML = self.getHTML(url, charset='euc-kr')
                article = self.getArticle(articleHTML)
                article.update(self.getArticleSummary(oid=params['oid'][0], aid=params['aid'][0]))
                obj['article'] = article
                articles.append(obj)
            except UnicodeDecodeError as e:
                print('getArticles UnicodeDecodeError')
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

if __name__ == "__main__":
    nhnNews = NaverNews('20170929')
    repo = MongoManager(database=nhnNews.database)
    con = {'site': nhnNews.site, 'date': nhnNews.date}

    if not repo.find_one(nhnNews.collection, con):
        links = nhnNews.getArticleLinks()
        # print(links)
        articles = nhnNews.getArticles(links)
        print('links len: %d, articles len: %d' % (len(links), len(articles)))

        nhnNews.save(articles, repo)
    else:
        for article in repo.find(nhnNews.collection, con):
            print(article)











