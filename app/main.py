import logging
import os
import time
from parser.base_parser import BaseParser
from parser.category_parser import CategoryParser
from parser.recommendation_parser import RecommendationParser
from parser.review_parser import ReviewParser
from parser.shop_parser import ShopParser

import psycopg2
from psycopg2 import OperationalError

# Logging setup
logging.basicConfig(
    filename="errors.log",
    filemode="w",
    format="%(message)s",
    level=logging.ERROR,
)

DATA_PATH = os.path.join(os.path.dirname(__file__), "data")


def create_conn(host, database, user, password, port):
    connection = None
    while connection is None:
        try:
            connection = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port,
            )
            print("Connection to PostgreSQL DB successful")
        except OperationalError:
            print("PostgreSQL not ready yet. Waiting...")
            time.sleep(2)

    return connection


if __name__ == "__main__":
    conn = create_conn(
        "db",
        os.getenv("POSTGRES_DB"),
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        "5432",
    )
    cursor = conn.cursor()

    error_counts = {}

    leipzig = f"{DATA_PATH}/leipzig_transformed.xml"
    dresden = f"{DATA_PATH}/dresden.xml"

    # Leipzig
    ShopParser(conn, cursor, leipzig).parse()
    ShopParser(conn, cursor, dresden).parse()

    # Dresden
    RecommendationParser(conn, cursor, leipzig).parse()
    RecommendationParser(conn, cursor, dresden).parse()

    # Categories
    CategoryParser(conn, cursor, f"{DATA_PATH}/categories.xml").parse()
    # Reviews
    ReviewParser(conn, cursor, f"{DATA_PATH}/reviews.csv").parse()

    for code, count in BaseParser.error_counts.items():
        logging.error(f"Error {code}: {count}")

    conn.commit()
    conn.close()
