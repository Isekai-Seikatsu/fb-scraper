# -*- coding: utf-8 -*-
import scrapy

from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from scrapy.shell import inspect_response

import json


class FanPageSpider(scrapy.Spider):
    name = 'fan_page'
    allowed_domains = ['www.facebook.com']
    start_urls = ['https://www.facebook.com/BAMBOOVIII/']

    def parse(self, response):

        update_link: str = self.extract_link(response)
        self.logger.info('First request sent')

        yield response.follow(update_link, callback=self.update_link_parse)

    
    def update_link_parse(self, response):

        update: dict = json.loads(response.text[9:])
        assert (update['domops'][0][0] == 'replace'
                and update['domops'][0][1] == '#www_pages_reaction_see_more_unitwww_pages_home'
                and update['domops'][0][2] == False), 'Update action changed!'

        html = update['domops'][0][3]['__html']
        sel = Selector(text=html)
        
        for utime in sel.xpath('//@data-utime').getall():
            self.logger.info(f'utime: {utime}')

        # inspect_response(response, self)
        self.logger.info(f'------ segment ------')
        update_link: str = self.extract_link(sel)
        yield response.follow(update_link, callback=self.update_link_parse)


    
    @staticmethod
    def extract_link(selector) -> str:
        return (selector
                .xpath('//*[@id="www_pages_reaction_see_more_unitwww_pages_home"]//@ajaxify')
                .get()
                + '&__a=1')

        


        

        

