# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


import os
import logging
from dotenv import load_dotenv

import mysql.connector
from mysql.connector import IntegrityError

from googletrans import Translator


# Load environmental variables
load_dotenv(".env")


class TypesPipeline:
    def process_item(self, item, spider):
        item["ID"] = int(item["ID"])
        item["Payment"] = float(item.get("Payment", "0"))
        item["Postal"] = float(item.get("Postal", "0"))
        item["Square"] = float(item.get("Square", "0"))

        return item


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
        logging.log(logging.INFO, "AWS pipeline init")
        self.connection = mysql.connector.connect(

            host=os.getenv("AWS_DB_ENDPOINT"),
            port=os.getenv("AWS_MYSQL_PORT"),
            user=os.getenv("AWS_DB_USER"),
            password=os.getenv("AWS_DB_PASSWORD"),
            database=os.getenv("AWS_DB_NAME")

        )
        logging.log(logging.INFO, "AWS pipeline connection established")
        self.cur = self.connection.cursor()
        self.create_table()

    def create_table(self):
        logging.log(logging.INFO, "AWS pipeline tables creation")
        self.cur.execute(
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
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS images(
                URL TEXT ,
                MESSAGE_ID INT NOT NULL
            )
            """
        )
        logging.log(logging.INFO, "AWS pipeline tables created")

    def process_item(self, item, spider):
        logging.log(logging.INFO, "AWS pipeline item processing")
        try:
            self.cur.execute("""
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
            logging.log(logging.INFO, "AWS pipeline estate message added")
            print("hello")
            for img_url in item.get("image_urls", []):

                print(img_url)

                self.cur.execute(
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
                logging.log(logging.INFO, "AWS pipeline img message added")

        except IntegrityError as e:
            print(e)
        else:
            self.connection.commit()
            logging.log(logging.INFO, "AWS pipeline additions commited")

        return item
