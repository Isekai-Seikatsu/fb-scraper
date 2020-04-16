# -*- coding: utf-8 -*-
import datetime
import json
from urllib.parse import parse_qs, urlencode, urlparse

import scrapy
from pymongo import MongoClient
from scrapy.http import Request


class ReactorsSpider(scrapy.Spider):
    name = 'reactors'
    allowed_domains = ['m.facebook.com']

    REACTION_TYPE_MAPPING = {'1': 'like', '2': 'love',
                             '3': 'wow', '4': 'haha', '7': 'sorry', '8': 'anger'}

    custom_settings = {
        'URLLENGTH_LIMIT': 131072,
        'ITEM_PIPELINES': {
            'public_fan_page.pipelines.MongoPostReactorsPipeline': 100
        }
    }

    def start_requests(self):
        self.client = MongoClient(self.settings.get('MONGO_URI'))
        self.db = self.client.get_default_database()

        secret_cookies = json.loads(self.settings.get('SECRET_FB_COOKIES'))
        for post in self.db.post.find(projection={'post_id': 1, '_id': 0}):
            post_id = post['post_id']
            yield Request(
                f'https://m.facebook.com/ufi/reaction/profile/browser/?ft_ent_identifier={post_id}',
                cookies=secret_cookies,
                cb_kwargs={'post_id': post_id}
            )

    def parse(self, response, post_id):
        urls = response.css(
            'a[role="button"][href^="/ufi/reaction/profile/browser/fetch/"]::attr("href")').getall()
        start_time = datetime.datetime.utcnow()
        start_date = datetime.datetime.combine(start_time, datetime.time())

        reaction_types = list(self.get_reaction_types(urls))
        self.db.history.post_reactors.update_one(
            {'post_id': post_id, 'date': start_date},
            {
                '$push': {
                    'hist': {
                        'fetched_time': {'start': start_time},
                        'reactors': [{
                            'type': reaction_type,
                            'uids': []
                        } for _, reaction_type in reaction_types]
                    }
                }
            },
            upsert=True
        )

        for url, reaction_type in reaction_types:
            yield response.follow(
                url, callback=self.reactor_parse,
                cb_kwargs={
                    'reaction_type': reaction_type,
                    'post_id': post_id,
                    'start_time': start_time,
                    'start_date': start_date
                }
            )

    def reactor_parse(self, response, **kwargs):
        main_block = response.xpath('//ul/li/table/tbody/tr/td')

        item = {
            **kwargs,
            'reactors': []
        }

        for reactor_block in main_block.xpath('table/tbody/tr'):
            tds = reactor_block.xpath('td')
            profile_link = tds.xpath('descendant::h3/a/@href')
            # If it's invalid , considering `tds.xpath('*[self::div|self::header]/h3/a/@href')``

            reactor = {
                'profile_link': profile_link.get(),
                'name': tds.xpath('descendant::h3/a/text()').get(),
            }

            action_link = tds.xpath('div/a/@href').get()
            if action_link:
                pr = urlparse(action_link)
                uid = parse_qs(pr.query)['id'][0]
                reactor['action_path'] = pr.path
                reactor['uid'] = uid
            else:
                uid = profile_link.re('/profile\.php\?id=(\d+)')
                if uid:
                    reactor['uid'] = uid[0]

            if 'uid' in reactor:
                reactor['uid'] = int(reactor['uid'])

            item['reactors'].append(reactor)

        if item['reactors']:
            yield item

        next_link = main_block.xpath('div/a/@href').get()
        if next_link:
            pr = urlparse(next_link)
            d = parse_qs(pr.query)
            d['limit'] = ['800']
            query = urlencode(d, doseq=True)
            path = pr._replace(query=query).geturl()

            yield response.follow(
                path, callback=self.reactor_parse,
                cb_kwargs=kwargs
            )
        else:
            self.logger.info(
                f"Post_id[{kwargs['post_id']}], Reaction[{kwargs['reaction_type']}] Scraped Done!")

    def get_reaction_types(self, urls):
        for url in urls:
            parse_result = urlparse(url)
            params: dict = parse_qs(parse_result.query)
            if 'reaction_type' in params:
                yield (url, self.REACTION_TYPE_MAPPING[params['reaction_type'][0]])
