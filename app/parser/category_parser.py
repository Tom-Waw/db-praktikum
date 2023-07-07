import traceback
import xml.etree.ElementTree as ET
from parser.base_parser import BaseParser


class CategoryParser(BaseParser):
    TABLE_NAME = "categories"
    REL_TABLE_NAME = "product_category"
    PRODUCT_TABLE_NAME = "products"

    def __init__(self, conn, cursor, path: str):
        super().__init__(conn, cursor)
        self.root = ET.parse(path).getroot()

    def parse(self, root=None, parent_id: int = None):
        if root is None:
            root = self.root

        for item in root:
            if item.tag == "category":
                self.parse_category(item, parent_id)
            elif item.tag == "item":
                self.parse_item(item.text, parent_id)

    def parse_category(self, item, parent_id):
        name = item.text.strip()
        if not name:
            self.log_error(9, "Category", "name", "Missing name")
            return

        try:
            category_id = self.get_or_insert_id(
                self.TABLE_NAME,
                {
                    "name": name,
                    "parent_id": parent_id,
                },
                ["name", "parent_id"],
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(10, name, "INSERT Category", str(e))
            return

        self.parse(item, category_id)

    def parse_item(self, item, parent_id):
        asin = item.text.strip()
        if not asin:
            self.log_error(11, "CategoryItem", "asin", "Missing ASIN")
            return

        product_id = self.fetch_from_table(self.PRODUCT_TABLE_NAME, {"asin": asin})
        if product_id is None:
            self.log_error(12, asin, "product", "Missing product")
            return

        fetch_data = {
            "product_id": product_id,
            "category_id": parent_id,
        }
        if (
            self.fetch_from_table(self.REL_TABLE_NAME, fetch_data, columns=["*"])
            is not None
        ):
            return

        try:
            self.get_or_insert_id(
                self.REL_TABLE_NAME,
                {
                    "product_id": product_id,
                    "category_id": parent_id,
                },
                ["product_id", "category_id"],
                return_id=False,
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(13, asin, "INSERT ProductCategory", str(e))
