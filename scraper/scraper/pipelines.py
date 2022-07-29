# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


import os
import logging
from dotenv import load_dotenv

from scrapy.exceptions import DropItem

import mysql.connector
from mysql.connector import IntegrityError, InterfaceError, DatabaseError

from googletrans import Translator


# Load environmental variables
load_dotenv("scraper/.env")


class TypesPipeline:
    def process_item(self, item, spider):
        item["ID"] = int(item["ID"])
        item["Payment"] = float(item.get("Payment", "0"))
        item["Postal"] = float(item.get("Postal", "0"))
        item["Square"] = float(item.get("Square", "0"))

        return item


class RegisterPipeline:

    def __init__(self):

        logging.log(logging.INFO, "Registration pipeline: Initialization")

        self.connection = mysql.connector.connect(

            host=os.getenv("AWS_DB_ENDPOINT"),
            port=os.getenv("AWS_MYSQL_PORT"),
            user=os.getenv("AWS_DB_USER"),
            password=os.getenv("AWS_DB_PASSWORD"),
            database=os.getenv("AWS_DB_NAME")

        )

        logging.log(
            logging.INFO, "Registration pipeline: AWS connection established")

        self.cursor = self.connection.cursor()

        self.prev_ids = set()
        self.cur_ids = set()

        try:
            self.cursor.execute(
                """
                SELECT ID FROM estate;
                """
            )

            self.prev_ids = {x[0] for x in self.cursor.fetchall()}
        except InterfaceError as e:
            self.prev_ids = {}

    def process_item(self, item, spider):

        logging.log(logging.INFO, "Registration pipeline: Checking item")

        self.cur_ids.add(item["ID"])

        if item["ID"] in self.prev_ids:
            raise DropItem(
                "Registration pipeline: Item is already in database")

        logging.log(
            logging.INFO, "Registration pipeline: Item registration is successfull")

        return item

    def close_spider(self, spider):

        logging.log(
            logging.INFO, "Registration pipeline: Checkout of outdated items")

        outdated_ids = self.prev_ids - self.cur_ids

        logging.log(
            logging.INFO, f"{len(outdated_ids)} listings are outdated.")

        try:
            for outdated_id in outdated_ids:
                self.cursor.execute(
                    f"""
                    
                DELETE FROM estate WHERE ID = {str(outdated_id)}
                
                """
                )
        except DatabaseError:
            logging.INFO, "Registration pipeline: Deletion is not completed, try again"
        else:
            self.connection.commit()
            logging.log(logging.INFO, "Outdated listings successfully deleted")


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


class AWSMySQLPipeline:

    def __init__(self):
        self.connection = mysql.connector.connect(

            host=os.getenv("AWS_DB_ENDPOINT"),
            port=os.getenv("AWS_MYSQL_PORT"),
            user=os.getenv("AWS_DB_USER"),
            password=os.getenv("AWS_DB_PASSWORD"),
            database=os.getenv("AWS_DB_NAME")

        )

        logging.log(logging.INFO, "AWS pipeline connection established")

        self.cursor = self.connection.cursor()
        logging.log(logging.INFO, "AWS pipeline initialization")
        self.create_table()

    def create_table(self):
        logging.log(logging.INFO, "AWS pipeline tables creation")
        self.cursor.execute(
            """

            CREATE TABLE IF NOT EXISTS estate(
                ID              INT PRIMARY KEY NOT NULL,
                TITLE           TEXT,
                TOWN            TEXT,
                DESCRIPTION     TEXT,
                POSTING_INFO    TEXT,
                TYPE            TEXT,
                PAYMENT         REAL,
                POSTAL          INT,
                ADDRESS         TEXT,
                SQUARE          REAL,
                N_ROOMS         REAL,
                URL             TEXT
                )
            """
        )
        self.cursor.execute(
            """

            CREATE TABLE IF NOT EXISTS images(
                URL TEXT,
                MESSAGE_ID INT NOT NULL
            )

            """
        )

        logging.log(logging.INFO, "AWS pipeline tables are created")

    def process_item(self, item, spider):
        logging.log(logging.INFO, "AWS pipeline item processing")
        try:
            self.cursor.execute("""
                INSERT INTO estate
                    (   ID,
                        TITLE,
                        TOWN,
                        DESCRIPTION,
                        POSTING_INFO,
                        TYPE,
                        PAYMENT,
                        POSTAL,
                        ADDRESS,
                        SQUARE,
                        N_ROOMS,
                        URL)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                (
                                    item.get("ID", ""),
                                    item.get("Title", ""),
                                    item.get("Town", ""),
                                    item.get("Description", ""),
                                    item.get("Posting_info", ""),
                                    item.get("Type", ""),
                                    item.get("Payment", ""),
                                    item.get("Postal", ""),
                                    item.get("Address", ""),
                                    item.get("Square", ""),
                                    item.get("N_rooms", ""),
                                    item.get("URL", "")
                                ))

            logging.log(logging.INFO, "Estate listing is added")

            for img_url in item.get("image_urls", []):

                print(img_url)

                self.cursor.execute(
                    """
                    INSERT INTO images(
                        URL,
                        MESSAGE_ID
                    )
                    VALUES (%s,%s)
                    """,
                    (
                        img_url,
                        item.get("ID", "")
                    )
                )
                logging.log(logging.INFO, "Images are added")

        except IntegrityError as e:
            print(e)
        else:
            self.connection.commit()
            logging.log(logging.INFO, "Changes are successfully commited")

        return item
