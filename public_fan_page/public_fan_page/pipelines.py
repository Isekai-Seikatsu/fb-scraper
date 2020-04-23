import abc
import asyncio
import datetime

import pytz
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError, OperationFailure
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
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client.get_default_database()

    def close_spider(self, spider):
        self.client.close()

    @abc.abstractmethod
    def process_item(self, item, spider):
        pass


class MongoFanPagePipeline(MongoPipelineABC):
    EMPTY_TIME = datetime.time()

    async def process_item(self, item, spider):
        utc_now = datetime.datetime.utcnow()
        utc_date = datetime.datetime.combine(utc_now, self.EMPTY_TIME)
        post_id = int(item['fbid'])

        hist_reactions_col = ('share_count', 'comment_count', 'reaction_count')
        hist_reactions_item = {col: item[col] for col in hist_reactions_col}
        hist_reactions_item['fetched_time'] = utc_now
        hist_reactions_item['reactions'] = [{'type': k, 'count': v}
                                            for k, v in item['reactions'].items()]

        hist_coll = self.db.history
        result_fu = hist_coll.post_reactions.update_one(
            {'post_id': post_id, 'date': utc_date},
            {
                '$push': {
                    'hist': hist_reactions_item
                }
            },
            upsert=True
        )

        posted_time = (datetime.datetime
                       .fromtimestamp(int(item['posted_time']))
                       .astimezone(pytz.timezone(spider.settings.get('TIMEZONE'))))

        resp_fu = self.db.post.find_one_and_update(
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

        result: UpdateResult
        result, resp = await asyncio.gather(result_fu, resp_fu)

        spider.logger.info(result.raw_result)

        if resp is None:
            spider.logger.info('Post upsert happend')
        else:
            if resp['msg'] != item['msg']:
                # update msg history
                result: UpdateResult = await hist_coll.post_msg.update_one(
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
    async def process_item(self, item, spider):
        ids_fus = [
            self.db.fb_user.find_one_and_update(
                doc,
                {'$currentDate': {'update_time': True}},
                projection={'_id': 1},
                return_document=True,
                upsert=True
            )
            for doc in item['reactors']
        ]

        ids = [doc['_id'] for doc in await asyncio.gather(*ids_fus)]

        update_result = await self.db.history.post_reactors.update_one(
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

class MongoFBUserUidPipeline(MongoPipelineABC):
    async def process_item(self, item, spider):

        result = await self.db.fb_user.update_one(
            {'_id': item['_id']},
            {
                '$set': {
                    'uid': item['uid']
                },
                '$currentDate': {
                    'update_time': True
                }
            }
        )
        spider.logger.info(result.raw_result)
        return item