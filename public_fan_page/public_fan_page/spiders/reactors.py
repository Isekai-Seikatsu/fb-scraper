# -*- coding: utf-8 -*-
import json
from urllib.parse import parse_qs, urlparse, urlencode

import scrapy
from scrapy.http import Request


class ReactorsSpider(scrapy.Spider):
    name = 'reactors'
    allowed_domains = ['m.facebook.com']

    start_urls = ['1741331419254052']
    REACTION_TYPE_MAPPING = {'1': 'like', '2': 'love',
                             '3': 'wow', '4': 'haha', '7': 'sorry', '8': 'anger'}

    custom_settings = {
        'URLLENGTH_LIMIT': 131072
    }

    def start_requests(self):
        secret_cookies = json.loads(self.settings.get('SECRET_FB_COOKIES'))
        for post_id in self.start_urls:
            yield Request(
                f'https://m.facebook.com/ufi/reaction/profile/browser/?ft_ent_identifier={post_id}',
                cookies=secret_cookies,
                cb_kwargs={'post_id': post_id}
            )

    def parse(self, response, post_id):
        urls = response.css(
            'a[role="button"][href^="/ufi/reaction/profile/browser/fetch/"]::attr("href")').getall()
        for url in urls:
            parse_result = urlparse(url)
            params: dict = parse_qs(parse_result.query)
            if 'reaction_type' in params:
                yield response.follow(
                    url, callback=self.reactor_parse,
                    cb_kwargs={
                        'reaction_type': self.REACTION_TYPE_MAPPING[params['reaction_type'][0]],
                        'post_id': post_id
                    }
                )

    def reactor_parse(self, response, reaction_type, post_id):
        
        main_block = response.xpath('//ul/li/table/tbody/tr/td')

        for reactor_block in main_block.xpath('table/tbody/tr'):
            tds = reactor_block.xpath('td')
            profile_link = tds.xpath('div/h3/a/@href')

            reactor = {
                'post_id': post_id,
                'reaction_type': reaction_type,
                'profile_link': profile_link.get(),
                'name': tds.xpath('div/h3/a/text()').get(),
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

            yield reactor

        next_link = main_block.xpath('div/a/@href').get()
        if next_link:
            pr = urlparse(next_link)
            d = parse_qs(pr.query)
            d['limit'] = ['800']
            query = urlencode(d, doseq=True)
            path = pr._replace(query=query).geturl()

            yield response.follow(
                path, callback=self.reactor_parse,
                cb_kwargs={
                    'reaction_type': reaction_type,
                    'post_id': post_id
                }
            )
        else:
            self.logger.info(f'Reaction[{reaction_type}] Scraped Done!')
