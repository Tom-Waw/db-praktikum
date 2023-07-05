import csv
from datetime import datetime
import logging
import os
import traceback
import xml.etree.ElementTree as ET

import mysql.connector

logging.basicConfig(filename="errors.log", filemode="w", level=logging.ERROR)

logger = logging.getLogger()
logger.handlers[0].setFormatter(logging.Formatter("%(message)s\n"))


class DataLoader:
    DATA_PATH = os.path.join(os.path.dirname(__file__), "data/")
    error_counts = {}

    def __init__(self):
        self.conn = mysql.connector.connect(
            host="db",
            user="root",
            password=os.getenv("DATABASE_PASSWORD"),
            port=3306,
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute("USE media_store;")

    def log_error(self, code, entity, attribute, message):
        logger.error(f"{entity} ({attribute}): {code} {message}")
        self.conn.rollback()

        if code not in self.error_counts:
            self.error_counts[code] = 0

        self.error_counts[code] += 1

    def load(self):
        # Leipzig
        self.parse_and_create_shop(
            ET.parse(f"{self.DATA_PATH}/leipzig_transformed.xml").getroot()
        )
        self.parse_and_create_shop(ET.parse(f"{self.DATA_PATH}/dresden.xml").getroot())

        # Dresden
        self.parse_and_create_recommendations(
            ET.parse(f"{self.DATA_PATH}/leipzig_transformed.xml").getroot()
        )
        self.parse_and_create_recommendations(
            ET.parse(f"{self.DATA_PATH}/dresden.xml").getroot()
        )

        # Categories
        self.parse_categories(ET.parse(f"{self.DATA_PATH}/categories.xml").getroot())
        # Reviews
        self.parse_reviews(f"{self.DATA_PATH}/reviews.csv")

        for code, count in self.error_counts.items():
            logger.error(f"Error {code}: {count}")

        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def parse_reviews(self, path):
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row["product"]
                if not asin:
                    self.log_error(1, "Review", "asin", "Missing ASIN")
                    continue

                rating = int(x) if (x := row["rating"]) else None
                if not rating or rating < 1 or rating > 5:
                    self.log_error(2, asin, "rating", "Missing rating")
                    continue

                customer_name = row["user"]
                if not customer_name:
                    self.log_error(3, asin, "customer_name", "Missing customer name")
                    continue

                summary = row["summary"]
                if not summary:
                    self.log_error(4, asin, "summary", "Missing summary")
                    continue

                content = row["content"]
                if not content:
                    self.log_error(5, asin, "content", "Missing content")
                    continue

                sql = "SELECT id FROM product WHERE asin = %s"
                val = (asin,)
                self.cursor.execute(sql, val)
                if (product_id := self.cursor.fetchone()) is None:
                    self.log_error(6, asin, "product", "Missing reviewed product")
                    continue

                product_id = product_id[0]

                sql = "SELECT id FROM customer WHERE name = %s"
                val = (customer_name,)
                self.cursor.execute(sql, val)
                if (customer_id := self.cursor.fetchone()) is None:
                    try:
                        sql = "INSERT INTO customer (name) VALUES (%s)"
                        val = (customer_name,)
                        self.cursor.execute(sql, val)
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(7, customer_name, "INSERT Customer", e)
                        continue

                    customer_id = self.cursor.lastrowid
                else:
                    customer_id = customer_id[0]

                sql = "SELECT * FROM review WHERE customer_id = %s AND product_id = %s"
                val = (customer_id, product_id)
                self.cursor.execute(sql, val)
                if self.cursor.fetchone() is not None:
                    continue

                try:
                    sql = "INSERT INTO review (customer_id, product_id, rating, summary, content) VALUES (%s, %s, %s, %s, %s)"
                    val = (
                        customer_id,
                        product_id,
                        rating,
                        summary,
                        content,
                    )
                    self.cursor.execute(sql, val)
                    self.conn.commit()
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(8, asin, "INSERT Review", e)
                    continue

    def parse_categories(self, root, parent_id=None):
        for item in root:
            if item.tag == "category":
                name = item.text.strip()
                if not name:
                    self.log_error(9, "Category", "name", "Missing name")
                    continue

                sql = (
                    f"SELECT id FROM category WHERE name = %s AND parent_id = %s"
                    if parent_id
                    else f"SELECT id FROM category WHERE name = %s AND parent_id IS NULL"
                )
                val = (name, parent_id) if parent_id else (name,)
                self.cursor.execute(sql, val)
                if (category_id := self.cursor.fetchone()) is None:
                    try:
                        sql = "INSERT INTO category (name, parent_id) VALUES (%s, %s)"
                        val = (name, parent_id)
                        self.cursor.execute(sql, val)
                        self.conn.commit()
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(10, name, "INSERT Category", e)
                        continue

                    category_id = self.cursor.lastrowid
                else:
                    category_id = category_id[0]

                self.parse_categories(item, category_id)

            elif item.tag == "item":
                asin = item.text
                if not asin:
                    self.log_error(11, "CategoryItem", "asin", "Missing ASIN")
                    continue

                sql = "SELECT id FROM product WHERE asin = %s"
                val = (asin,)
                self.cursor.execute(sql, val)
                if (product_id := self.cursor.fetchone()) is None:
                    self.log_error(12, asin, "product", "Missing product")
                    continue

                product_id = product_id[0]

                sql = "SELECT * FROM product_category WHERE product_id = %s AND category_id = %s"
                val = (product_id, parent_id)
                self.cursor.execute(sql, val)
                if self.cursor.fetchone() is None:
                    try:
                        sql = "INSERT INTO product_category (product_id, category_id) VALUES (%s, %s)"
                        val = (product_id, parent_id)
                        self.cursor.execute(sql, val)
                        self.conn.commit()
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(13, asin, "INSERT ProductCategory", e)
                        continue

    def parse_and_create_item(self, root, branch_id):
        # product info
        asin = root.attrib.pop("asin", None)
        if not asin:
            self.log_error(14, "Unknown", "asin", "Missing ASIN")
            return

        pgroup = root.attrib.pop("pgroup", None)
        if pgroup not in ["Music", "DVD", "Book"]:
            self.log_error(15, asin, "pgroup", "Unknown product group")
            return

        name = x.text if (x := root.find("title")) is not None else None
        if not name:
            self.log_error(16, asin, "name", "Missing name")
            return

        image = root.attrib.pop("picture", None)
        sales_rank = int(x) if (x := root.attrib.pop("salesrank", None)) else None

        sql = "SELECT id FROM product WHERE asin = %s"
        val = (asin,)
        self.cursor.execute(sql, val)
        if (product_id := self.cursor.fetchone()) is None:
            try:
                sql = "INSERT INTO `product` (`asin`, `name`, `image`, `rank`) VALUES (%s, %s, %s, %s)"
                val = (asin, name, image, sales_rank)
                self.cursor.execute(sql, val)
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(17, asin, "INSERT", e)
                return

            product_id = self.cursor.lastrowid
        else:
            product_id = product_id[0]

        if pgroup == "Music":
            label = ""
            for x in root.find("labels"):
                l = x.attrib.pop("name", None)
                if l and len(label) < len(l):
                    label = l
            if not label:
                self.log_error(18, asin, "label", "Missing label")
                return

            date_published = (
                datetime.strptime(x, "%Y-%m-%d").date()
                if (x := root.find("musicspec").find("releasedate").text)
                else None
            )
            if date_published is None:
                self.log_error(19, asin, "date_published", "Missing date published")
                return

            tracks = [track.text for track in root.find("tracks")]
            artists = [artist.attrib.pop("name") for artist in root.find("artists")]

            sql = "SELECT id FROM cd WHERE id = %s"
            val = (product_id,)
            self.cursor.execute(sql, val)
            if self.cursor.fetchone() is None:
                try:
                    sql = (
                        "INSERT INTO cd (id, label, date_published) VALUES (%s, %s, %s)"
                    )

                    val = (product_id, label, date_published)
                    self.cursor.execute(sql, val)
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(20, asin, "INSERT CD", e)
                    return

                try:
                    sql = "INSERT INTO track (cd_id, title) VALUES (%s, %s)"
                    val = [(product_id, t) for t in tracks]
                    self.cursor.executemany(sql, val)
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(21, asin, "INSERT Tracks", e)
                    return

                for artist in artists:
                    sql = "SELECT id FROM person WHERE name = %s"
                    val = (artist,)
                    self.cursor.execute(sql, val)
                    if (artist_id := self.cursor.fetchone()) is None:
                        try:
                            sql = "INSERT INTO person (name) VALUES (%s)"
                            val = (artist,)
                            self.cursor.execute(sql, val)
                        except Exception as e:
                            print(traceback.format_exc())
                            self.log_error(22, artist, "INSERT Artist", e)
                            continue

                        artist_id = self.cursor.lastrowid
                    else:
                        artist_id = artist_id[0]

                    try:
                        sql = "INSERT INTO person_product (person_id, product_id) VALUES (%s, %s)"
                        val = (artist_id, product_id)
                        self.cursor.execute(sql, val)
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(23, artist, "INSERT Artist to Product", e)
                        continue

        elif pgroup == "DVD":
            dvd_data = root.find("dvdspec")
            format = dvd_data.find("format").text
            if not format:
                self.log_error(24, asin, "format", "Unknown format")
                return

            duration = int(x) if (x := dvd_data.find("runningtime").text) else None
            if duration is None:
                self.log_error(25, asin, "duration", "Missing duration")
                return

            region_code = dvd_data.find("regioncode").text
            if not region_code:
                self.log_error(26, asin, "region_code", "Missing region code")
                return

            involved_people = [
                (n, r)
                for p, r in [
                    ("actors", "ACTOR"),
                    ("creators", "CREATOR"),
                    ("directors", "DIRECTOR"),
                ]
                for x in root.find(p)
                if (n := x.attrib.pop("name", None))
            ]

            sql = "SELECT id FROM dvd WHERE id = %s"
            val = (product_id,)
            self.cursor.execute(sql, val)
            if self.cursor.fetchone() is None:
                try:
                    sql = "INSERT INTO dvd (id, format, duration, region_code) VALUES (%s, %s, %s, %s)"
                    val = (product_id, format, duration, region_code)
                    self.cursor.execute(sql, val)
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(27, asin, "INSERT DVD", e)
                    return

                for person, role in involved_people:
                    sql = "SELECT id FROM person WHERE name = %s"
                    val = (person,)
                    self.cursor.execute(sql, val)
                    if (person_id := self.cursor.fetchone()) is None:
                        try:
                            sql = "INSERT INTO person (name) VALUES (%s)"
                            val = (person,)
                            self.cursor.execute(sql, val)
                        except Exception as e:
                            print(traceback.format_exc())
                            self.log_error(28, person, "INSERT Person", e)
                            continue

                        person_id = self.cursor.lastrowid
                    else:
                        person_id = person_id[0]

                    sql = "SELECT * FROM person_product WHERE person_id = %s AND product_id = %s AND role = %s"
                    val = (person_id, product_id, role)
                    self.cursor.execute(sql, val)
                    if self.cursor.fetchone() is None:
                        try:
                            sql = "INSERT INTO person_product (person_id, product_id, role) VALUES (%s, %s, %s)"
                            val = (person_id, product_id, role)
                            self.cursor.execute(sql, val)
                        except Exception as e:
                            print(traceback.format_exc())
                            self.log_error(29, person, "INSERT Person to Product", e)
                            continue

        elif pgroup == "Book":
            book_data = root.find("bookspec")
            isbn = book_data.find("isbn").attrib.pop("val", None)
            if not isbn:
                self.log_error(30, asin, "isbn", "Missing ISBN")
                return

            n_pages = int(x) if (x := book_data.find("pages").text) else None
            if n_pages is None:
                self.log_error(31, asin, "n_pages", "Missing number of pages")
                return

            date_published = (
                datetime.strptime(x, "%Y-%m-%d").date()
                if (x := book_data.find("publication").attrib.pop("date"))
                else None
            )
            if date_published is None:
                self.log_error(32, asin, "date_published", "Missing date published")
                return

            publisher = [
                n for p in root.find("publishers") if (n := p.attrib.pop("name", None))
            ]
            publisher = publisher[0] if publisher else None

            if not publisher:
                self.log_error(33, asin, "publisher", "Missing publisher")
                return

            authors = [a.attrib.pop("name") for a in root.find("authors")]

            if publisher is not None:
                sql = "SELECT id FROM publisher WHERE name = %s"
                val = (publisher,)
                self.cursor.execute(sql, val)
                if (publisher_id := self.cursor.fetchone()) is None:
                    try:
                        sql = "INSERT INTO publisher (name) VALUES (%s)"
                        val = (publisher,)
                        self.cursor.execute(sql, val)
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(34, publisher, "INSERT Publisher", e)
                        return

                    publisher_id = self.cursor.lastrowid
                else:
                    publisher_id = publisher_id[0]

            sql = "SELECT id FROM book WHERE id = %s"
            val = (product_id,)
            self.cursor.execute(sql, val)
            if self.cursor.fetchone() is None:
                try:
                    sql = """
                        INSERT INTO book (id, isbn, n_pages, date_published, publisher_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    val = (product_id, isbn, n_pages, date_published, publisher_id)
                    self.cursor.execute(sql, val)
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(35, asin, "INSERT Book", e)
                    return

            for a in authors:
                sql = "SELECT id FROM person WHERE name = %s"
                val = (a,)
                self.cursor.execute(sql, val)
                if (author_id := self.cursor.fetchone()) is None:
                    try:
                        sql = "INSERT INTO person (name) VALUES (%s)"
                        val = (a,)
                        self.cursor.execute(sql, val)
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(36, a, "INSERT Author", e)
                        continue

                    author_id = self.cursor.lastrowid
                else:
                    author_id = author_id[0]

                try:
                    sql = "INSERT INTO person_product (person_id, product_id) VALUES (%s, %s)"
                    val = (author_id, product_id)
                    self.cursor.execute(sql, val)
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(37, a, "INSERT Author to Book", e)
                    continue

        # sale info
        price_data = root.find("price")
        price = float(x) if (x := price_data.text) else None
        stock = bool(price)

        state = price_data.attrib.pop("state").upper()
        if state not in ["NEW", "USED"]:
            self.log_error(38, asin, "state", "Invalid state")
            return

        sql = "SELECT id FROM branch_product WHERE product_id = %s AND branch_id = %s AND state = %s"
        val = (product_id, branch_id, state)
        self.cursor.execute(sql, val)
        if self.cursor.fetchone() is None:
            try:
                sql = "INSERT INTO branch_product (product_id, branch_id, price, state, stock) VALUES (%s, %s, %s, %s, %s)"
                val = (product_id, branch_id, price, state, stock)
                self.cursor.execute(sql, val)
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(39, asin, "INSERT Branch to Product", e)
                return

    def parse_and_create_shop(self, root):
        name = root.attrib.pop("name", None)
        street = root.attrib.pop("street", None)
        zipcode = root.attrib.pop("zip", None)

        if not name or not street or not zipcode:
            self.log_error(40, name, "name", "Missing name, street, or zipcode")
            return

        sql = "SELECT id FROM branch WHERE name = %s"
        val = (name,)
        self.cursor.execute(sql, val)
        if (branch_id := self.cursor.fetchone()) is None:
            try:
                sql = "INSERT INTO address (street, zip) VALUES (%s, %s)"
                val = (street, zipcode)
                self.cursor.execute(sql, val)
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(41, name, "INSERT Address", e)
                return

            address_id = self.cursor.lastrowid

            try:
                sql = "INSERT INTO branch (name, address_id) VALUES (%s, %s)"
                val = (name, address_id)
                self.cursor.execute(sql, val)
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(42, name, "INSERT Branch", e)
                return

            branch_id = self.cursor.lastrowid
        else:
            branch_id = branch_id[0]

        self.conn.commit()

        for item in root:
            self.parse_and_create_item(item, branch_id)
            self.conn.commit()

    def parse_and_create_recommendations(self, root):
        for item in root:
            asin = item.attrib.pop("asin", None)
            if not asin:
                self.log_error(43, asin, "asin", "Missing asin")
                continue

            sql = "SELECT id FROM product WHERE asin = %s"
            val = (asin,)
            self.cursor.execute(sql, val)
            if (product_id := self.cursor.fetchone()) is None:
                self.log_error(44, asin, "asin", "ASIN not found")
                continue

            product_id = product_id[0]

            for recommendation in item.find("similars"):
                asin = x.text if (x := recommendation.find("asin")) else None
                if not asin:
                    self.log_error(45, asin, "asin", "Missing asin in recommendation")
                    continue

                sql = "SELECT id FROM product WHERE asin = %s"
                val = (asin,)
                self.cursor.execute(sql, val)
                if (rec_id := self.cursor.fetchone()) is None:
                    self.log_error(46, asin, "asin", "Recommendation ASIN not found")
                    continue

                rec_id = rec_id[0]

                sql = "SELECT * FROM recommendation WHERE product_id = %s AND recommended_product_id = %s"
                val = (product_id, rec_id)
                self.cursor.execute(sql, val)
                if self.cursor.fetchone() is None:
                    try:
                        sql = "INSERT INTO recommendation (product_id, recommended_product_id) VALUES (%s, %s)"
                        val = (product_id, rec_id)
                        self.cursor.execute(sql, val)
                    except Exception as e:
                        print(traceback.format_exc())
                        self.log_error(47, asin, "INSERT Recommendation", e)
                        continue

                self.conn.commit()


if __name__ == "__main__":
    DataLoader().load()
