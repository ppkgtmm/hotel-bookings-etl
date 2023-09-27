from typing import List
from dotenv import load_dotenv
from os import getenv

load_dotenv()

db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_port = getenv("DB_PORT")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")


def get_connection_str():
    return {
        "oltp": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{oltp_db}",
        "olap": f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{olap_db}",
    }


def get_insert_query(table_name: str, fields: List[str]):
    query = "INSERT INTO {}".format(table_name)
    columns = " (" + ", ".join(fields) + ") "
    values = "VALUES (" + ", ".join([":" + field for field in fields]) + ")"
    return query + columns + values
