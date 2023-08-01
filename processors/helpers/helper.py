from dotenv import load_dotenv
from os import getenv

# from sqlalchemy import create_engine, text
# from datetime import datetime, timedelta
from pyspark.sql.types import StringType, MapType


class ProcessingHelper:
    def __init__(self) -> None:
        load_dotenv()
        db_host = getenv("DB_HOST_INTERNAL")
        db_port = getenv("DB_PORT")
        db_user = getenv("DB_USER")
        db_password = getenv("DB_PASSWORD")
        db_name = getenv("OLAP_DB")
        self.connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
            db_user, db_password, db_host, db_port, db_name
        )
        self.json_schema = MapType(StringType, StringType)

    # @staticmethod
    # def to_datetime(date_time):
    #     return datetime.fromtimestamp(date_time / 1000)

    # @staticmethod
    # def to_date(days):
    #     return datetime(1970, 1, 1) + timedelta(days=days)

    # @classmethod
    # def upsert_to_db(cls, table_name, payload, columns):
    #     query = f"""INSERT INTO {table_name} ({', '.join(columns)})
    #                 VALUES ({', '.join([':'+col for col in columns])})
    #                 ON DUPLICATE KEY UPDATE {', '.join([col+'=:'+col for col in columns])}
    #             """
    #     cls.conn.execute(text(query), payload)
    #     cls.conn.commit()

    # @staticmethod
    # def prepare_payload(payload):
    #     payload["_id"] = payload.pop("id")
    #     return payload

    # @classmethod
    # def insert_to_db(cls, table_name, payload, columns, prepare=True):
    #     query = f"""INSERT INTO {table_name} ({', '.join(columns)})
    #                 VALUES ({', '.join([':'+col for col in columns])})
    #             """
    #     if prepare:
    #         payload = ProcessingHelper.prepare_payload(payload)
    #     cls.conn.execute(text(query), payload)
    #     cls.conn.commit()

    # def close(self, error):
    #     if error:
    #         print("Closed with error: %s" % str(error))
    #     ProcessingHelper.conn.close()
    #     ProcessingHelper.engine.dispose()
