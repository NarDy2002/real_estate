
from scrapy.crawler import CrawlerProcess
from scraper.spiders import estate
from scrapy.utils.project import get_project_settings

import logging

logging.basicConfig(
    filename='log.txt',
    format='%(levelname)s: %(message)s',
    level=logging.INFO
)

custom_settings = {
    "ITEM_PIPELINES": {
        'scraper.pipelines.TranslatePipeline': 400,
    },

    "FEEDS": {
        'items.json': {
            'format': 'json',
            'encoding': 'utf8',
            'store_empty': False,
            'indent': 4,
            'item_export_kwargs': {
                'export_empty_fields': True,
            }
        }
    },
    "CONCURRENT_REQUESTS": 32,
}

process = CrawlerProcess(
    settings=custom_settings
)


if __name__ == "__main__":
    process.crawl(estate.RealestateSpider)
    process.start()
