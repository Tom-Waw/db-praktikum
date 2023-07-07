import logging
from abc import ABC, abstractmethod

import psycopg2


class BaseParser(ABC):
    error_counts = {}

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def log_error(self, code, entity, attribute, message):
        # logging.error(f"{entity} ({attribute}): {code} {message}")
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

    def get_or_insert_id(self, table_name, data, key_columns, return_id=True):
        if len(key_columns) > 0:
            select_columns = ", ".join(key_columns)

            select_values = tuple(data[key] for key in key_columns)
            select_condition = " AND ".join(
                [
                    f"{key} IS NULL" if value is None else f"{key} = %s"
                    for key, value in zip(key_columns, select_values)
                ]
            )
            select_query = (
                f"SELECT {select_columns} FROM {table_name} WHERE {select_condition}"
            )

            self.cursor.execute(select_query, select_values)
            result = self.cursor.fetchone()

            if result is not None:
                return result[0] if len(result) > 0 else None

        columns = data.keys()
        values = tuple(data.values())

        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
        if return_id:
            insert_query += " RETURNING id"

        self.cursor.execute(insert_query, values)

        if return_id:
            inserted_id = self.cursor.fetchone()
            return inserted_id[0] if inserted_id is not None else None

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
    def insert_into_table(self, table_name, data: dict, return_id=False):
        columns, values = zip(*data.items())
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
        if return_id:
            query += " RETURNING id"

        self.execute_sql(query, values)

    @abstractmethod
    def parse(self):
        ...
