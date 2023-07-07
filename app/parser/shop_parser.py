import traceback
import xml.etree.ElementTree as ET
from datetime import datetime
from parser.base_parser import BaseParser


class ShopParser(BaseParser):
    ADDRESS_TABLE_NAME = "addresses"
    SHOP_TABLE_NAME = "shops"
    REL_TABLE_NAME = "shop_product"
    PRODUCT_TABLE_NAME = "products"

    PERSON_TABLE_NAME = "persons"
    PERSON_REL_TABLE_NAME = "person_product"
    PUBLISHER_TABLE_NAME = "publishers"
    TRACK_TABLE_NAME = "tracks"

    CD_TABLE_NAME = "cds"
    DVD_TABLE_NAME = "dvds"
    BOOK_TABLE_NAME = "books"

    def __init__(self, conn, cursor, path: str):
        super().__init__(conn, cursor)
        self.root = ET.parse(path).getroot()

    def parse(self):
        self.conn.autocommit = False

        name = self.root.attrib.get("name", None)
        street = self.root.attrib.get("street", None)
        zipcode = self.root.attrib.get("zip", None)

        if not name or not street or not zipcode:
            self.log_error(40, name, "name", "Missing name, street, or zipcode")
            return

        shop_id = self.fetch_from_table(self.SHOP_TABLE_NAME, {"name": name})
        if shop_id is None:
            try:
                self.insert_into_table(
                    self.ADDRESS_TABLE_NAME,
                    {
                        "street": street,
                        "zip": zipcode,
                    },
                )
                address_id = self.cursor.lastrowid
                print(address_id)
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(41, name, "INSERT Address", str(e))
                return

            try:
                self.insert_into_table(
                    self.SHOP_TABLE_NAME,
                    {
                        "name": name,
                        "address_id": address_id,
                    },
                )
                shop_id = self.cursor.lastrowid
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(42, name, "INSERT shop", str(e))
                return

        self.conn.commit()

        for item in self.root:
            self.parse_and_create_item(item, shop_id)

        self.conn.autocommit = True

    def parse_and_create_item(self, root, shop_id):
        asin = root.attrib.get("asin", None)
        if not asin:
            self.log_error(14, "Unknown", "asin", "Missing ASIN")
            return

        pgroup = root.attrib.get("pgroup", None)
        if pgroup not in ["Music", "DVD", "Book"]:
            self.log_error(15, asin, "pgroup", "Unknown product group")
            return

        name = x.text if (x := root.find("title")) is not None else None
        if not name:
            self.log_error(16, asin, "name", "Missing name")
            return

        image = root.attrib.get("picture", None)
        sales_rank = int(x) if (x := root.attrib.pop("salesrank", None)) else None

        product_id = self.fetch_from_table(self.PRODUCT_TABLE_NAME, {"asin": asin})
        if product_id is None:
            try:
                self.insert_into_table(
                    self.PRODUCT_TABLE_NAME,
                    {
                        "asin": asin,
                        "name": name,
                        "image": image,
                        "rank": sales_rank,
                    },
                )
                product_id = self.cursor.lastrowid
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(17, asin, "INSERT", str(e))
                return

        if pgroup == "Music":
            self.handle_music(root, asin, product_id)
        elif pgroup == "DVD":
            self.handle_dvd(root, asin, product_id)
        elif pgroup == "Book":
            self.handle_book(root, asin, product_id)

        self.conn.commit()

        price_data = root.find("price")
        price = float(x) if (x := price_data.text) else None
        stock = bool(price)

        state = price_data.attrib.get("state").upper()
        if state not in ["NEW", "USED"]:
            self.log_error(38, asin, "state", "Invalid state")
            return

        if self.fetch_from_table(self.REL_TABLE_NAME, data, columns=["*"]) is not None:
            return

        try:
            data = {
                "product_id": product_id,
                "shop_id": shop_id,
                "price": price,
                "state": state,
                "stock": stock,
            }
            self.insert_into_table(self.REL_TABLE_NAME, data)
            self.conn.commit()
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(39, asin, "INSERT Branch to Product", str(e))

    def handle_music(self, item, asin, product_id):
        if (
            self.fetch_from_table(
                self.CD_TABLE_NAME,
                {"product_id": product_id},
                columns=["*"],
            )
            is not None
        ):
            return

        label = max(
            [x.attrib.get("name", "") for x in item.find("labels")],
            key=len,
            default="",
        )
        if not label:
            self.log_error(18, asin, "label", "Missing label")
            return

        date_published = (
            datetime.strptime(x, "%Y-%m-%d").date()
            if (x := item.find("musicspec").find("releasedate").text)
            else None
        )
        if date_published is None:
            self.log_error(19, asin, "date_published", "Missing date published")
            return

        tracks = [track.text for track in item.find("tracks")]
        artists = [artist.attrib.get("name") for artist in item.find("artists")]

        try:
            self.insert_into_table(
                self.CD_TABLE_NAME,
                {
                    "product_id": product_id,
                    "label": label,
                    "date_published": date_published,
                },
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(20, asin, "INSERT CD", str(e))
            return

        for track in tracks:
            try:
                self.insert_into_table(
                    self.TRACK_TABLE_NAME,
                    {"cd_id": product_id, "title": track},
                )
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(21, asin, f"INSERT Track: {track}", str(e))

        for artist in artists:
            artist_id = self.fetch_from_table(self.PERSON_TABLE_NAME, {"name": artist})
            if artist_id is None:
                try:
                    self.insert_into_table(self.PERSON_TABLE_NAME, {"name": artist})
                    artist_id = self.cursor.lastrowid
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(22, artist, f"INSERT Artist: {artist}", str(e))
                    continue

            try:
                self.insert_into_table(
                    self.PERSON_REL_TABLE_NAME,
                    {"person_id": artist_id, "product_id": product_id},
                )
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(23, artist, f"INSERT Artist to Product {artist}", str(e))
                continue

    def handle_dvd(self, item, asin, product_id):
        if (
            self.fetch_from_table(
                self.DVD_TABLE_NAME,
                {"product_id": product_id},
                columns=["*"],
            )
            is not None
        ):
            return

        dvd_data = item.find("dvdspec")
        format = dvd_data.find("format").text
        if not format:
            self.log_error(24, asin, "format", "Unknown format")
            return

        duration_str = dvd_data.find("runningtime").text
        if not duration_str:
            self.log_error(25, asin, "duration", "Missing duration")
            return
        duration = int(duration_str)

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
            for x in item.find(p)
            if (n := x.attrib.get("name", None))
        ]

        try:
            self.insert_into_table(
                self.DVD_TABLE_NAME,
                {
                    "product_id": product_id,
                    "format": format,
                    "duration": duration,
                    "region_code": region_code,
                },
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(27, asin, "INSERT DVD", str(e))
            return

        for person, role in involved_people:
            person_id = self.fetch_from_table(self.PERSON_TABLE_NAME, {"name": person})
            if person_id is None:
                try:
                    self.insert_into_table(self.PERSON_TABLE_NAME, {"name": person})
                    person_id = self.cursor.lastrowid
                except Exception as e:
                    print(traceback.format_exc())
                    self.log_error(28, person, "INSERT Person", str(e))
                    continue

            if (
                self.fetch_from_table(
                    self.PERSON_REL_TABLE_NAME,
                    {
                        "person_id": person_id,
                        "product_id": product_id,
                        "role": role,
                    },
                    columns=["*"],
                )
                is not None
            ):
                continue

            try:
                self.insert_into_table(
                    self.PERSON_REL_TABLE_NAME,
                    {
                        "person_id": person_id,
                        "product_id": product_id,
                        "role": role,
                    },
                )
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(29, person, "INSERT Person to Product", str(e))
                continue

    def handle_book(self, item, asin, product_id):
        if (
            self.fetch_from_table(
                self.BOOK_TABLE_NAME,
                {"product_id": product_id},
                columns=["*"],
            )
            is not None
        ):
            return

        book_data = item.find("bookspec")
        isbn = book_data.find("isbn").attrib.get("val", None)
        if not isbn:
            self.log_error(30, asin, "isbn", "Missing ISBN")
            return

        n_pages_str = book_data.find("pages").text
        if not n_pages:
            self.log_error(31, asin, "n_pages", "Missing number of pages")
            return
        n_pages = int(n_pages_str)

        date_published = (
            datetime.strptime(x, "%Y-%m-%d").date()
            if (x := book_data.find("publication").attrib.pop("date"))
            else None
        )
        if date_published is None:
            self.log_error(32, asin, "date_published", "Missing date published")
            return

        publisher = None
        for p in item.find("publishers"):
            if publisher := p.attrib.get("name", None):
                break
        if not publisher:
            self.log_error(33, asin, "publisher", "Missing publisher")
            return

        authors = [a.attrib.pop("name") for a in item.find("authors")]

        publisher_id = self.fetch_from_table(
            self.PUBLISHER_TABLE_NAME,
            {"name": publisher},
        )
        if publisher_id is None:
            try:
                self.insert_into_table(self.PUBLISHER_TABLE_NAME, {"name": publisher})
                publisher_id = self.cursor.lastrowid
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(34, publisher, "INSERT Publisher", str(e))
                return

        try:
            self.insert_into_table(
                self.BOOK_TABLE_NAME,
                {
                    "product_id": product_id,
                    "isbn": isbn,
                    "n_pages": n_pages,
                    "date_published": date_published,
                    "publisher_id": publisher_id,
                },
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(35, asin, "INSERT Book", str(e))
            return

        for author in authors:
            author_id = self.fetch_from_table(self.PERSON_TABLE_NAME, {"name": author})
            if author_id is not None:
                continue

            try:
                self.insert_into_table(
                    self.PERSON_REL_TABLE_NAME,
                    {"person_id": author_id, "product_id": product_id},
                )
                author_id = self.cursor.lastrowid
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(36, author, "INSERT Author", str(e))
                continue

            try:
                self.insert_into_table(
                    self.PERSON_REL_TABLE_NAME,
                    {
                        "person_id": author_id,
                        "product_id": product_id,
                        "role": "AUTHOR",
                    },
                )
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(37, author, "INSERT Author to Book", str(e))
                continue
