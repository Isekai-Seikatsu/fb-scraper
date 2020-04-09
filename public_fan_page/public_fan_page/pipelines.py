import abc
import datetime

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
            # Make sure that connection through URI is success
            self.client.admin.command('ismaster')
        except OperationFailure as opfail:
            self.logger.error(opfail)
            raise opfail

    def close_spider(self, spider):
        self.client.close()

    @abc.abstractmethod
    def process_item(self, item, spider):
        pass


class MongoFanPagePipeline(MongoPipelineABC):
    EMPTY_TIME = datetime.time()

    def process_item(self, item, spider):
        utc_now = datetime.datetime.utcnow()
        utc_date = datetime.datetime.combine(utc_now, self.EMPTY_TIME)
        post_id = int(item['fbid'])

        hist_reactions_col = ('share_count', 'comment_count', 'reaction_count')
        hist_reactions_item = {col: item[col] for col in hist_reactions_col}
        hist_reactions_item['fetched_time'] = utc_now
        hist_reactions_item['reactions'] = [{'type': k, 'count': v}
                                            for k, v in item['reactions'].items()]

        hist_coll = self.db.history
        hist_coll.post_reactions.update_one(
            {'post_id': post_id, 'date': utc_date},
            {
                '$addToSet': {
                    'hist': hist_reactions_item
                }
            },
            upsert=True
        )
        return item
