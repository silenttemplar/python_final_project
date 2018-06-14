from abc import *
import urllib.parse as parse
import urllib.request as req

class News(metaclass=ABCMeta):
    NAVER, DAUM = range(2)
    collection = 'news'

    def createUrl(self, params):
        return self.baseUrl + '?' + parse.urlencode(params)

    def getHTML(self, url, charset='utf-8'):
        data = req.urlopen(url).read()
        html = data.decode(charset)
        return html

    def getArticleLinks(self, html):
        pass

    def parseNewsLinks(self, html):
        pass

    def getArticle(self, html):
        pass

    def getArticles(self, links):
        pass

    def save(self, items, manager):
        pass





