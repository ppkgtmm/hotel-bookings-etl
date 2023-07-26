import json
from datetime import datetime
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
olap_db = getenv("OLAP_DB")

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{olap_db}"
)
# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     StringType,
#     IntegerType,
#     TimestampType,
# )

# schema = StructType(
#     [
#         StructField("id", IntegerType()),
#         StructField("state", StringType()),
#         StructField("country", StringType()),
#         StructField("created_at", TimestampType()),
#         StructField("updated_at", TimestampType()),
#     ]
# )


def to_timestamp(date_time: str, fmt: str = "%Y-%m-%dT%H:%M:%SZ"):
    return datetime.strptime(date_time, fmt)


class LocationProcessor:
    columns = ["id", "state", "country", "created_at", "updated_at"]

    def open(self):
        LocationProcessor.engine = create_engine(connection_string)
        LocationProcessor.conn = LocationProcessor.engine.connect()
        return True

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.location":
            return
        payload = json.loads(row.value)["payload"]["after"]
        payload["created_at"] = to_timestamp(payload["created_at"])
        payload["updated_at"] = to_timestamp(payload["updated_at"])
        columns = LocationProcessor.columns
        # values = [payload[col] for col in columns]
        query = f"""INSERT INTO stg_location ({', '.join(columns)}) VALUES ({', '.join(':'+columns)})
                    ON DUPLICATE KEY UPDATE {', '.join([col+'= :'+col for col in columns])}
                """
        LocationProcessor.conn.execute(text(query), **payload)
        LocationProcessor.conn.commit()

    def close(self, error):
        LocationProcessor.conn.close()
        LocationProcessor.engine.dispose()
        if error:
            print("Closed with error: %s" % str(error))
