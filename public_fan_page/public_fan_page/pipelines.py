import abc

import pymongo
from pymongo.errors import OperationFailure


class PublicFanPagePipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipelineABC(abc.ABC):

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mongo_uri=crawler.settings.get('MONGO_URI'))

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client.get_default_database()
        try:
            self.client.admin.command('ismaster')
        except OperationFailure as opfail:
            self.logger.error(opfail)
        
    def close_spider(self, spider):
        self.client.close()

    @abc.abstractmethod
    def process_item(self, item, spider):
        pass


class MongoFanPagePipeline(MongoPipelineABC):
    def process_item(self, item, spider):
        return item
