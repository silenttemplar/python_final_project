from pymongo import MongoClient

import json

class MongoManager(object):
    url = 'mongodb://127.0.0.1:27017'

    def __init__(self, url=None, database=None):
        self.client = MongoClient(MongoManager.url if not url else url)
        self.database = 'repo' if not database else database
        self.db = self.client[self.database]

    def __del__(self):
        self.client.close()
        # print('MongoManager delete!')

    def save_one(self, collection, data, database=None):
        db = self.client[database] if database else self.db
        # db[collection]
        return db[collection].insert_one(data).inserted_id

    def save(self, collection, data, database=None):
        db = self.client[database] if database else self.db
        # db[collection]
        return db[collection].insert_many(data).inserted_ids

    def find(self, collection, con, database=None):
        db = self.client[database] if database else self.db
        return db[collection].find(con)

    def find_one(self, collection, con, database=None):
        db = self.client[database] if database else self.db

        return db[collection].find_one(con)

    def push(self, collection, con, data, database=None):
        db = self.client[database] if database else self.db
        result = db[collection].update(con, {'$push': data})
        # print(result)
        return result['n'] if 'n' in result else -1

if __name__ == '__main__':
    repo = MongoManager(database='exam')
    mydata = """
    {"name": "yundream", "age":35, "'4.1'":1}
    """
    d = json.loads(mydata)
    print('mydata: ', mydata, 'd: ', d)
    id = repo.save_one('test', d)
    print('id: ', id)

    print('find_one: ', repo.find_one('test', {"_id": id}))



