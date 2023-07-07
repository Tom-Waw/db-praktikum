import logging
from abc import ABC, abstractmethod

import psycopg2


class BaseParser(ABC):
    error_counts = {}

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def log_error(self, code, entity, attribute, message):
        logging.error(f"{entity} ({attribute}): {code} {message}")
        self.conn.rollback()

        if code not in BaseParser.error_counts:
            BaseParser.error_counts[code] = 0

        BaseParser.error_counts[code] += 1

    # Function to handle all sql queries
    def execute_sql(self, query, params=None):
        try:
            self.cursor.execute(query, params)
        except psycopg2.Error as e:
            logging.error(f"Failed to execute SQL: {e}")
            self.conn.rollback()
            raise

    # Function to handle all sql select queries
    def fetch_from_table(self, table_name, data, columns=None):
        select = "id" if columns is None else "1"

        condition = " AND ".join(
            [
                f"{key} IS NULL" if value is None else f"{key} = %s"
                for key, value in data.items()
            ]
        )

        self.cursor.execute(
            f"SELECT {select} FROM {table_name} WHERE {condition}",
            tuple(data.values()),
        )
        return res[0] if (res := self.cursor.fetchone()) is not None else None

    # Function to handle all sql insert queries
    def insert_into_table(self, table_name, data: dict):
        columns, values = zip(*data.items())
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
        self.execute_sql(query, values)

    @abstractmethod
    def parse(self):
        ...
