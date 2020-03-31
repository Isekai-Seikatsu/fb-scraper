import json
from urllib.parse import urlparse

import scrapy
from scrapy.selector import Selector


class FanPageSpider(scrapy.Spider):
    name = 'fan_page'
    allowed_domains = ['www.facebook.com']
    start_urls = ['https://www.facebook.com/BAMBOOVIII/']

    def parse(self, response):
        update_link: str = self.extract_link(response)
        self.logger.info('First request sent')
        yield response.follow(update_link, callback=self.update_link_parse,
                              cb_kwargs={'fan_page': response.url})

    def update_link_parse(self, response, **kwargs):

        update: dict = json.loads(response.text[9:])    # Slice `for (;;);` off
        assert (update['domops'][0][0] == 'replace'
                and update['domops'][0][1] == '#www_pages_reaction_see_more_unitwww_pages_home'
                and update['domops'][0][2] == False), 'Update action changed!'

        html = update['domops'][0][3]['__html']
        sel = Selector(text=html)

        posts_info_from_html = [{
            'posted_time': post.xpath('*//@data-utime').get(),
            'msg': ''.join(post.css('[data-testid="post_message"] ::text').getall()),
            'url_path': urlparse(post.xpath('*//@data-utime/parent::*/parent::a/@href').get()).path,
            'fbid': post.css('input[type="hidden"][name="ft_ent_identifier"]::attr("value")').get(),
        } for post in sel.css('.userContentWrapper')]

        predisplay_items = [item[3][1]['__bbox']['result']['data']['feedback']
                            for item in update['jsmods']['pre_display_requires']
                            if (item[0] == 'RelayPrefetchedStreamCache' and item[1] == 'next')]

        posts_info_from_predisplay = [{
            'url': data['url'],
            'share_count': data['share_count']['count'],
            'comment_count': data['comment_count']['total_count'],
            'reaction_count': data['reaction_count']['count'],
            'fbid': data['share_fbid']
        } for data in predisplay_items]

        agg_items = {post['fbid']: post for post in posts_info_from_html}
        for item in posts_info_from_predisplay:
            if item['fbid'] in agg_items:
                agg_items[item['fbid']].update(item)
            else:
                raise Exception('posts_info_from_predisplay not handle')

        yield from agg_items.values()

        self.logger.info(f'------ segment ------')
        update_link = sel.xpath(
            '//*[@id="www_pages_reaction_see_more_unitwww_pages_home"]//@ajaxify').get()

        if update_link:
            update_link += '&__a=1'
            headers = {
                'Referer': kwargs['fan_page']
            }
            yield response.follow(update_link, callback=self.update_link_parse, headers=headers,
                                  cb_kwargs=kwargs)

    @staticmethod
    def extract_link(selector) -> str:
        return (selector
                .xpath('//*[@id="www_pages_reaction_see_more_unitwww_pages_home"]//@ajaxify')
                .get()
                + '&__a=1')
