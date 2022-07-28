import scrapy
from scrapy.loader import ItemLoader
from scraper.items import ScraperItem


def get_number_of_pages(text: str) -> int:
    """
    Gets number of estates from given string holder 
    and counts on how many pages these estates are located on.

    To get number of pages, number of estates is divided by *100* +1 page for leftover

    Returns: Number of pages
    """
    number_of_estates = int(text.split(' ')[0].replace(f"\'", ""))
    return number_of_estates // 100 + 1


class RealestateSpider(scrapy.Spider):
    name = 'estate'
    allowed_domains = ['www.tutti.ch']

    def start_requests(self):

        cities = (
            'bern',
            'zurich',
            'basel'
        )

        for city in cities:
            url = f"https://www.tutti.ch/de/immobilien/objekttyp/wohnungen,hauser/standort/ort-{city}/typ/mieten"
            yield scrapy.Request(url=url, callback=self.parse_town)

    def parse_town(self, response):

        number_of_pages = get_number_of_pages(
            response.css("h1.Wcvxq::text").get())

        for i in range(1, number_of_pages + 1):
            url = response.url + f"?paging={i}"
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        for link in response.css("a.e1hvux6w5::attr(href)").getall():
            yield response.follow(link, callback=self.parse_estates)

    def parse_estates(self, response):

        l = ItemLoader(item=ScraperItem(), selector=response)

        l.add_css("Title", "title::text")
        l.add_css("Description", "div.css-1rc7znr::text")
        l.add_css("Posting_info", 'div.pRm6L span::text')

        l.add_xpath("Type", '//dl[dt/text() = "Typ"]/dd/text()')
        l.add_xpath("Payment", '//dl[dt/text() = "Miete CHF"]/dd/text()')
        l.add_xpath("Postal", '//dl[dt/text() = "PLZ"]/dd/text()')
        l.add_xpath("Address", '//dl[dt/text() = "Adresse"]/dd/text()')
        l.add_xpath("Square", '//dl[dt/text() = "Fläche"]/dd/text()')
        l.add_xpath("N_rooms", '//dl[dt/text() = "Zimmer"]/dd/text()')

        l.add_value("ID", response.url.split("/")[-1])
        l.add_value('URL', response.url)
        l.add_value('Town', response.url.split("/")[5])
        l.add_value('image_urls',  response.css(
            "div.puEEg img[src*=jpg]::attr(src)").getall())

        yield l.load_item()
