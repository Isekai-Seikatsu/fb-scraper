import json
from base64 import b64encode

import requests
import scrapy
from scrapy.http import FormRequest, Request


class ReactorsSpider(scrapy.Spider):
    name = 'reactors'
    allowed_domains = ['www.facebook.com']
    REACTION_TYPES = ['LIKE', 'LOVE', 'WOW',
                      'HAHA', 'SORRY', 'ANGER', 'SUPPORT']
    API_BATCH_SIZE = 100

    def start_requests(self):
        self.secret_cookies = json.loads(
            self.settings.get('SECRET_FB_COOKIES'))

        api_input_url = getattr(self, 'api_input_url', None)
        reaction_types = getattr(self, 'reaction_types', None)
        if reaction_types:
            reaction_types = reaction_types.split(',')
        else:
            reaction_types = self.REACTION_TYPES

        if api_input_url:
            api_input = requests.get(self.api_input_url).json()
            yield Request(api_input[0]['url'], cookies=self.secret_cookies,
                          cb_kwargs={'reaction_types': reaction_types, 'post_ids': [data['post_id'] for data in api_input]})
        else:
            post_url = getattr(self, 'post_url', None)
            if not post_url:
                post_id = getattr(self, 'post_id', None)
                assert post_id, 'No post information provided!'
                post_url = f'https://www.facebook.com/{post_id}'

            yield Request(post_url, cookies=self.secret_cookies,
                          cb_kwargs={'reaction_types': reaction_types, 'post_ids': [post_id]})

    def parse(self, response, reaction_types, post_ids):
        self.token = response.css('script').re(
            '\["DTSGInitialData",\[\],\{"token":"([^"]*)"\}')[0]
        self.logger.info(f'TOKEN: {self.token}')

        for post_id in post_ids:
            for reaction_type in reaction_types:
                yield self.graphal_feedback_request(self.token, post_id, reaction_type)

    def feedback_data_parse(self, response, reaction_type, post_id):
        data = json.loads(response.text)['data']['node']
        assert data['__typename'] == 'Feedback'
        page_info = data['reactors']['page_info']

        for edge in data['reactors']['edges']:
            node = edge['node']
            yield {
                'uid': node['id'],
                'node_type': node['__typename'],
                'name': node['name'],
                'profile_url': node['profile_url'],
                'reaction': reaction_type,
                'post_id': post_id,
                # Whether to put profile_photo in or not
            }
            self.logger.debug(f"cursor: {edge['cursor']}")

        if page_info['has_next_page']:
            yield self.graphal_feedback_request(self.token, reaction_type,
                                                cursor=page_info['end_cursor'])
        else:
            self.logger.info(
                f"{reaction_type} End Cursor: {page_info['end_cursor']}")

    def graphal_feedback_request(self, token, post_id, reaction_type, cursor=None, count=None):
        id_ = b64encode(f'feedback:{post_id}'.encode()).decode()
        variables = {'count': count if count else self.API_BATCH_SIZE, 'cursor': cursor, 'feedbackTargetID': id_,
                     'reactionType': reaction_type, 'scale': 2, 'id': id_}
        return FormRequest(
            url="https://www.facebook.com/api/graphql/",
            formdata={'fb_dtsg': token, 'doc_id': '2796244503807763',
                      'variables': json.dumps(variables),
                      },
            cookies=self.secret_cookies,
            callback=self.feedback_data_parse,
            cb_kwargs={'reaction_type': reaction_type, 'post_id': post_id}
        )
