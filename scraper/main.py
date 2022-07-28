
from scrapy.crawler import CrawlerProcess
from scraper.spiders import estate
from scrapy.utils.project import get_project_settings

import logging

logging.basicConfig(
    filename='log.txt',
    format='%(levelname)s: %(message)s',
    level=logging.INFO
)

process = CrawlerProcess(
    settings=custom_settings
)


if __name__ == "__main__":
    process.crawl(estate.RealestateSpider)
    process.start()
