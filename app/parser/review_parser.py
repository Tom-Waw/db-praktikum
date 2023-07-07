import csv
import traceback
from parser.base_parser import BaseParser


class ReviewParser(BaseParser):
    REVIEW_TABLE_NAME = "reviews"
    CUSTOMER_TABLE_NAME = "customers"
    PRODUCT_TABLE_NAME = "products"

    def __init__(self, conn, cursor, path: str):
        super().__init__(conn, cursor)
        self.path = path

    def parse(self):
        with open(self.path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.parse_review(row)

    def parse_review(self, row):
        review_data = {
            "asin": row["product"],
            "rating": row["rating"],
            "user": row["user"],
            "summary": row["summary"],
            "content": row["content"],
        }

        for key, value in review_data.items():
            if value is None:
                self.log_error(1, review_data["asin"], key, f"Missing {key}")
                return

        product_id = self.fetch_from_table(
            self.PRODUCT_TABLE_NAME, {"asin": review_data["asin"]}
        )
        if product_id is None:
            self.log_error(6, review_data["asin"], "asin", "Missing reviewed product")
            return

        try:
            customer_id = self.get_or_insert_id(
                self.CUSTOMER_TABLE_NAME,
                {"name": review_data["user"]},
                ["name"],
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(7, review_data["user"], "INSERT Customer", str(e))
            return

        try:
            self.get_or_insert_id(
                self.REVIEW_TABLE_NAME,
                {
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "rating": int(review_data["rating"]),
                    "summary": review_data["summary"],
                    "content": review_data["content"],
                },
                [],
                return_id=False,
            )
        except Exception as e:
            print(traceback.format_exc())
            self.log_error(8, review_data["asin"], "INSERT Review", str(e))
