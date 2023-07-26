from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime


class Processor:
    @staticmethod
    def to_timestamp(date_time, fmt="%Y-%m-%dT%H:%M:%SZ"):
        return datetime.strptime(date_time, fmt)

    @classmethod
    def load_envars(cls):
        load_dotenv()
        cls.db_host = getenv("DB_HOST_INTERNAL")
        cls.db_port = getenv("DB_PORT")
        cls.db_user = getenv("DB_USER")
        cls.db_password = getenv("DB_PASSWORD")
        cls.db_name = getenv("OLAP_DB")

    @classmethod
    def setup_db_conn(cls):
        cls.connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
            cls.db_user, cls.db_password, cls.db_host, cls.db_port, cls.db_name
        )
        cls.engine = create_engine(cls.connection_string)
        cls.conn = cls.engine.connect()

    def open(self, partition_id, epoch_id):
        Processor.setup_db_conn()
        return True

    def close(self, error):
        if error:
            print("Closed with error: %s" % str(error))

    @classmethod
    def teardown_db_conn(cls):
        cls.conn.close()
        cls.engine.dispose()
