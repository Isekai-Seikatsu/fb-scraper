# -*- coding: utf-8 -*-
import json
from urllib.parse import parse_qs, urlparse

import scrapy
from pymongo import MongoClient
from scrapy.http import Request


class FillUpUidSpider(scrapy.Spider):
    name = 'fill_up_uid'
    allowed_domains = ['m.facebook.com']
    custom_settings = {
        'ITEM_PIPELINES': {
            'public_fan_page.pipelines.MongoFBUserUidPipeline': 100
        },
        'DOWNLOAD_DELAY': 150,
        'CONCURRENT_REQUESTS_PER_IP': 1
    }

    def start_requests(self):
        secret_cookies = json.loads(self.settings.get('SECRET_FB_COOKIES'))

        client = MongoClient(self.settings.get('MONGO_URI'))
        db = client.get_default_database()
        cursor = db.fb_user.find(
            {'uid': None}, projection={'profile_link': 1})

        for user in cursor:
            yield Request('https://m.facebook.com' + user['profile_link'],
                          cookies=secret_cookies,
                          cb_kwargs={'_id': user['_id']})

    def parse(self, response, _id):
        urls = response.xpath('//a/@href').getall()
        uid = self.get_uids(urls)
        assert uid, "uid is None"

        yield {
            '_id': _id,
            'uid': uid
        }

    @staticmethod
    def get_uids(urls):
        white_lists = ['owner_id', 'bid', 'id']
        for url in urls:
            pr = urlparse(url)
            qs = parse_qs(pr.query)
            for key in white_lists:
                if key in qs:
                    return int(qs[key])