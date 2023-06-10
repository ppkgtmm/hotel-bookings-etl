from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine, text

load_dotenv()

db_password_key = "DB_PASSWORD"

db_host = "localhost"
db_user = "root"
db_password = getenv(db_password_key)


sql_dir = "./db_setup"
oltp_sql = f"{sql_dir}/oltp_db.sql"
olap_sql = f"{sql_dir}/olap_db.sql"

connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:3306"


def get_queries(sql_file: str):
    with open(sql_file, "r") as fp:
        sql = fp.read()
    for q in sql.split(";"):
        if q.strip() == "":
            continue
        yield q


if __name__ == "__main__":
    engine = create_engine(connection_string)
    conn = engine.connect()

    print("-" * 30)
    print("setting up oltp database")
    for q in get_queries(oltp_sql):
        conn.execute(text(q))
    print("oltp database set up done")

    print("-" * 30)
    print("setting up olap database")
    for q in get_queries(olap_sql):
        conn.execute(text(q))
    print("olap database set up done")
    print("-" * 30)

    conn.close()
    engine.dispose()
