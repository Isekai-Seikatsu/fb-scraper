from twisted.internet import reactor
import scrapy
from scrapy.crawler import CrawlerRunner
from spiders.fan_page import FanPageSpider
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings


def run_crawl():
    deferred = runner.crawl(FanPageSpider)
    deferred.addCallback(lambda _: reactor.callLater(5, run_crawl))
    return deferred

runner = CrawlerRunner(get_project_settings())
configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
run_crawl()
# d = runner.crawl(FanPageSpider)
# d.addBoth(lambda _: reactor.stop())
reactor.run()