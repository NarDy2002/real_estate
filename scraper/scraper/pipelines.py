# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


from googletrans import Translator


# Load environmental variables
class TranslatePipeline:
    def process_item(self, item, spider):

        translator = Translator()

        # default language for translation - english, from - german

        item["Title"] = translator.translate(
            item.get("Title", ""), src="de").text
        item["Description"] = translator.translate(
            item.get("Description", ""), src="de").text
        item["Type"] = translator.translate(
            item.get("Type", ""), src="de").text

        return item