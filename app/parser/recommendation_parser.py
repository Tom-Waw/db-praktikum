import traceback
import xml.etree.ElementTree as ET
from parser.base_parser import BaseParser


class RecommendationParser(BaseParser):
    TABLE_NAME = "recommendations"
    PRODUCT_TABLE_NAME = "products"

    def __init__(self, conn, cursor, path: str):
        super().__init__(conn, cursor)
        self.root = ET.parse(path).getroot()

    def parse(self):
        for item in self.root:
            self.parse_recommendation(item)

    def parse_recommendation(self, item):
        asin = item.attrib.get("asin", None)
        if not asin:
            self.log_error(43, None, "asin", "Missing asin")
            return

        product_id = self.fetch_from_table(self.PRODUCT_TABLE_NAME, {"asin": asin})
        if product_id is None:
            self.log_error(44, asin, "asin", "ASIN not found")
            return

        for recommendation in item.find("similars"):
            asin_rec = x.text if (x := recommendation.find("asin")) else None
            if not asin_rec:
                self.log_error(45, asin_rec, "asin", "Missing asin in recommendation")
                return

            rec_id = self.fetch_from_table(self.PRODUCT_TABLE_NAME, {"asin": asin_rec})
            if rec_id is None:
                self.log_error(46, asin_rec, "asin", "Recommendation ASIN not found")
                return

            if (
                self.fetch_from_table(
                    self.TABLE_NAME,
                    {
                        "product_id": product_id,
                        "recommended_product_id": rec_id,
                    },
                    columns=["*"],
                )
                is not None
            ):
                continue

            try:
                self.insert_into_table(
                    self.TABLE_NAME,
                    {
                        "product_id": product_id,
                        "recommended_product_id": rec_id,
                    },
                )
            except Exception as e:
                print(traceback.format_exc())
                self.log_error(47, asin, "INSERT Recommendation", str(e))
                continue

            self.conn.commit()
