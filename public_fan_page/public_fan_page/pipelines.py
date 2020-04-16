import abc
import datetime

import pymongo
import pytz
from pymongo.errors import OperationFailure, DuplicateKeyError
from pymongo.results import UpdateResult


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
        result: UpdateResult = hist_coll.post_reactions.update_one(
            {'post_id': post_id, 'date': utc_date},
            {
                '$push': {
                    'hist': hist_reactions_item
                }
            },
            upsert=True
        )
        spider.logger.info(result.raw_result)

        posted_time = (datetime.datetime
                       .fromtimestamp(int(item['posted_time']))
                       .astimezone(pytz.timezone(spider.settings.get('TIMEZONE'))))

        resp = self.db.post.find_one_and_update(
            {
                'post_id': post_id,
                'page_id': int(item['page_id']),
                'url': item['url'],
                'url_path': item['url_path'],
                'posted_time': posted_time
            },
            {
                '$set': {
                    'msg': item['msg'],
                    'fetched_time': utc_now
                }
            },
            projection={'msg': True, 'fetched_time': True, '_id': False},
            upsert=True
        )

        if resp is None:
            spider.logger.info('Post upsert happend')
        else:
            if resp['msg'] != item['msg']:
                # update msg history
                result: UpdateResult = hist_coll.post_msg.update_one(
                    {'post_id': post_id},
                    {
                        '$push': {
                            'hist': {
                                'fetched_time': resp.get('fetched_time'),
                                'msg': resp.get('msg')
                            }
                        }
                    },
                    upsert=True
                )
                spider.logger.info(result.raw_result)

        return item


class MongoPostReactorsPipeline(MongoPipelineABC):
    def process_item(self, item, spider):

        ids = [
            self.db.fb_user.find_one_and_update(
                doc,
                {'$currentDate': {'update_time': True}},
                projection={'_id': 1},
                return_document=True,
                upsert=True
            )['_id'] for doc in item['reactors']]

        update_result = self.db.history.post_reactors.update_one(
            {'post_id': item['post_id'], 'date': item['start_date']},
            {
                '$push': {
                    'hist.$[histElem].reactors.$[reactor].uids': {
                        '$each': ids
                    }
                },
                '$currentDate': {
                    'hist.$[histElem].fetched_time.end': True
                }
            },
            array_filters=[{'histElem.fetched_time.start': item['start_time']},
                           {'reactor.type': item['reaction_type']}]
        )

        assert update_result.modified_count > 0, "uids not stored into history reactors"

        return item
