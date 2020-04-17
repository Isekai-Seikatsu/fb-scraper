from functools import partial

import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

from spiders.fan_page import FanPageSpider
from spiders.reactors import ReactorsSpider


def run_crawl(spider, interval=60):
    deferred = runner.crawl(spider)
    deferred.addCallback(
        lambda _: reactor.callLater(
            interval, partial(run_crawl, spider, interval))
    )
    return deferred


runner = CrawlerRunner(get_project_settings())
configure_logging(settings={'LOG_LEVEL': 'INFO'})

run_crawl(FanPageSpider)
run_crawl(ReactorsSpider, 600)
# d = runner.crawl(FanPageSpider)
# d.addBoth(lambda _: reactor.stop())
reactor.run()
