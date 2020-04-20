import json
from typing import List, Optional
from urllib.parse import urlparse

import scrapy
from pymongo import MongoClient
from scrapy.http import Request
from scrapy.selector import Selector


class FanPageSpider(scrapy.Spider):
    name = 'fan_page'
    allowed_domains = ['www.facebook.com']
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'public_fan_page.async_pipelines.MongoFanPagePipeline': 100
        }
    }

    def start_requests(self):
        client = MongoClient(self.settings.get('MONGO_URI'))
        db = client.get_default_database()
        cursor = db.fan_page.find(projection={'_id': 0})
        for page in cursor:
            yield Request(f"{page['link']}posts/")
        client.close()

    def parse(self, response):
        result: List[str] = response.css('script').re(
            'new \(require\("ServerJS"\)\)\(\).handle\((\{.*\})\);</script>')
        assert len(result) == 1, "First parse json regex error"

        pre_display_requires = json.loads(result[0])['pre_display_requires']
        yield from self.produce_items(response, pre_display_requires)

        update_link = self.extract_link(response)
        if update_link:
            yield response.follow(update_link, callback=self.update_link_parse,
                                  cb_kwargs={'fan_page': response.url})
        else:
            self.logger.info('Scrape Done when doing first parsing!')

    def update_link_parse(self, response, **kwargs):
        update: dict = json.loads(response.text[9:])    # Slice `for (;;);` off
        assert (update['domops'][0][0] == 'replace'
                and update['domops'][0][1] == '#www_pages_reaction_see_more_unitwww_pages_posts'
                and update['domops'][0][2] == False), 'Update action changed!'

        html = update['domops'][0][3]['__html']
        sel = Selector(text=html)
        pre_display_requires = update['jsmods']['pre_display_requires']

        yield from self.produce_items(sel, pre_display_requires)

        self.logger.info(f'------ item separate line ------')
        update_link = self.extract_link(sel)
        if update_link:
            headers = {
                'Referer': kwargs['fan_page']
            }
            yield response.follow(update_link, callback=self.update_link_parse, headers=headers,
                                  cb_kwargs=kwargs)
        else:
            self.logger.info('Scrape Done!')

    def produce_items(self, sel, pre_display_requires):
        posts_info_from_html = [{
            'posted_time': post.xpath('*//@data-utime').get(),
            'msg': ''.join(post.css('[data-testid="post_message"] ::text').getall()),
            'url_path': urlparse(post.xpath('*//@data-utime/parent::*/parent::a/@href').get()).path,
            'fbid': post.css('input[type="hidden"][name="ft_ent_identifier"]::attr("value")').get(),
        } for post in sel.css('.userContentWrapper')]

        predisplay_items = [item[3][1]['__bbox']['result']['data']['feedback']
                            for item in pre_display_requires
                            if (item[0] == 'RelayPrefetchedStreamCache' and item[1] == 'next')]

        posts_info_from_predisplay = [{
            'page_id': data['owning_profile']['id'],
            'url': data['url'],
            'share_count': data['share_count']['count'],
            'comment_count': data['comment_count']['total_count'],
            'reaction_count': data['reaction_count']['count'],
            'reactions': {
                reaction['node']['reaction_type'].lower(): reaction['reaction_count']
                for reaction in data['top_reactions']['edges']
            },
            'fbid': data['share_fbid']
        } for data in predisplay_items]

        agg_items = {post['fbid']: post for post in posts_info_from_html}
        for item in posts_info_from_predisplay:
            if item['fbid'] in agg_items:
                agg_items[item['fbid']].update(item)
            else:
                self.logger.info('posts_info_from_predisplay not handle')

        yield from agg_items.values()

    @staticmethod
    def extract_link(selector: Selector) -> Optional[str]:
        update_link = (selector
                       .xpath('//*[@id="www_pages_reaction_see_more_unitwww_pages_posts"]//@ajaxify')
                       .get())
        if update_link:
            return update_link + '&__a=1'
