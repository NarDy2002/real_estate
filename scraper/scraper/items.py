# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from datetime import date, timedelta
import scrapy
from itemloaders.processors import TakeFirst, MapCompose, Join


# from scraper.translate import translate
from datetime import date, timedelta
import scrapy
from itemloaders.processors import TakeFirst, MapCompose


# from scraper.translate import translate


def remove_squared_meters(string: str) -> str:
    return string.replace("m²", "")


def preprocess_square(text: str) -> str:
    return text.replace("m²", "")


def preprocess_date(text: str) -> str:
    # Serialize date posting info
    if "Gestern" in text:
        return (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")
    elif "Heute" in text:
        return date.today().strftime("%d.%m.%Y")
    else:
        return text


def preprocess_payment(text: str) -> str:
    # Takes only part with money amount, refactors it
    return text.split('.')[0].replace(f"\'", "")


def preprocess_address(text: str) -> str:
    result = str()
    for elem in text.split(' '):
        if elem.isalpha():
            result += elem + ' '
    return result


def remove_squared_meters(string: str) -> str:
    return string.replace("m²", "")


def preprocess_square(text: str) -> str:
    return text.replace("m²", "")


def preprocess_date(text: str) -> str:
    # Serialize date posting info
    if "Gestern" in text:
        return (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")
    elif "Heute" in text:
        return date.today().strftime("%d.%m.%Y")
    else:
        return text


def preprocess_payment(text: str) -> str:
    # Takes only part with money amount, refactors it
    return text.split('.')[0].replace(f"\'", "")


def preprocess_address(text: str) -> str:
    result = str()
    for elem in text.split(' '):
        if elem.isalpha():
            result += elem + ' '
    return result


class ScraperItem(scrapy.Item):
    # define the fields for your item here like:

    ID = scrapy.Field(output_processor=TakeFirst())
    Title = scrapy.Field(output_processor=TakeFirst())
    Description = scrapy.Field(output_processor=Join())
    Posting_info = scrapy.Field(input_processor=MapCompose(preprocess_date),
                                output_processor=TakeFirst())
    Type = scrapy.Field(output_processor=TakeFirst())
    Payment = scrapy.Field(input_processor=MapCompose(preprocess_payment),
                           output_processor=TakeFirst())
    Postal = scrapy.Field(output_processor=TakeFirst())
    Address = scrapy.Field(input_processor=MapCompose(preprocess_address),
                           output_processor=TakeFirst())
    Square = scrapy.Field(input_processor=MapCompose(preprocess_square),
                          output_processor=TakeFirst())
    N_rooms = scrapy.Field(output_processor=TakeFirst())

    URL = scrapy.Field(output_processor=TakeFirst())
    Town = scrapy.Field(output_processor=TakeFirst())
    image_urls = scrapy.Field()
