from dotenv import load_dotenv
from os import getenv, path
from sqlalchemy import create_engine, text


load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
dbz_user = getenv("DBZ_USER")
dbz_password = getenv("DBZ_PASSWORD")
dbz_previllages = getenv("DBZ_PREVILLAGES")

oltp_sql = path.join(path.abspath(path.dirname(__file__)), "sql/oltp_db.sql")
dwh_sql = path.join(path.abspath(path.dirname(__file__)), "sql/dwh_db.sql")

connection_str = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}"
debezium_sql = f"""
    DROP USER IF EXISTS {dbz_user};
    CREATE USER '{dbz_user}'@'%' IDENTIFIED BY '{dbz_password}';
    GRANT {dbz_previllages} ON *.* TO '{dbz_user}'@'%';
"""


def read_queries(sql_file: str):
    with open(sql_file, "r") as fp:
        queries = fp.read()
    return queries


def execute_queries(queries: str, connection_str: str):
    engine = create_engine(url=connection_str)
    connection = engine.connect()

    for query in queries.split(";"):
        if query.strip() == "":
            continue

        connection.execute(text(query))

    connection.close()
    engine.dispose()


if __name__ == "__main__":
    print("-" * 30)
    print("setting up oltp database")

    execute_queries(read_queries(oltp_sql), connection_str)

    print("oltp database set up done")
    print("-" * 30)
    print("setting up dwh database")

    execute_queries(read_queries(dwh_sql), connection_str)

    print("dwh database set up done")
    print("-" * 30)
    print("setting up debezium user")

    execute_queries(debezium_sql, connection_str)

    print("done setting up debezium user")
    print("-" * 30)
