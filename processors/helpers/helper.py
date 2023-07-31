from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta


class ProcessingHelper:
    @staticmethod
    def to_datetime(date_time):
        return datetime.fromtimestamp(date_time / 1000)

    @staticmethod
    def to_date(days):
        return datetime(1970, 1, 1) + timedelta(days=days)

    def open(self, partition_id, epoch_id):
        load_dotenv()
        db_host = getenv("DB_HOST_INTERNAL")
        db_port = getenv("DB_PORT")
        db_user = getenv("DB_USER")
        db_password = getenv("DB_PASSWORD")
        db_name = getenv("OLAP_DB")
        connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
            db_user, db_password, db_host, db_port, db_name
        )
        self.engine = create_engine(connection_string)
        self.conn = self.engine.connect()
        return True

    def upsert_to_db(self, table_name, payload, columns):
        query = f"""INSERT INTO {table_name} ({', '.join(columns)}) 
                    VALUES ({', '.join([':'+col for col in columns])})
                    ON DUPLICATE KEY UPDATE {', '.join([col+'=:'+col for col in columns])}
                """
        self.conn.execute(text(query), payload)
        self.conn.commit()

    @staticmethod
    def prepare_payload(payload):
        payload["_id"] = payload.pop("id")
        return payload

    def insert_to_db(self, table_name, payload, columns, prepare=True):
        query = f"""INSERT INTO {table_name} ({', '.join(columns)}) 
                    VALUES ({', '.join([':'+col for col in columns])})
                """
        if prepare:
            payload = ProcessingHelper.prepare_payload(payload)
        self.conn.execute(text(query), payload)
        self.conn.commit()

    def close(self, error):
        if error:
            print("Closed with error: %s" % str(error))
        self.conn.close()
        self.engine.dispose()
