from dotenv import load_dotenv
from os import getenv
from mysql import connector

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
dbz_user = getenv("DBZ_USER")
dbz_password = getenv("DBZ_PASSWORD")
dbz_previllages = getenv("DBZ_PREVILLAGES")

# sql_dir = "./db_setup"
# oltp_sql = f"{sql_dir}/oltp_db.sql"
# olap_sql = f"{sql_dir}/olap_db.sql"

oltp_sql = "oltp_db.sql"
olap_sql = "olap_db.sql"

connection_kwargs = dict(host=db_host, port=db_port, user=db_user, password=db_password)


def get_queries(sql_file: str):
    with open(sql_file, "r") as fp:
        sql = fp.read()
    for q in sql.split(";"):
        if q.strip() == "":
            continue
        yield q


if __name__ == "__main__":
    cnx = connector.connect(**connection_kwargs)
    cursor = cnx.cursor()
    # engine = create_engine(connection_string)
    # conn = engine.connect()

    print("-" * 30)
    print("setting up oltp database")

    # for q in get_queries(oltp_sql):
    #     conn.execute(q)

    print("oltp database set up done")
    print("-" * 30)
    print("setting up olap database")

    # for q in get_queries(olap_sql):
    #     conn.execute(q)

    print("olap database set up done")
    print("-" * 30)
    print("setting up debezium user")

    # conn.execute(f"CREATE USER '{dbz_user}'@'%' IDENTIFIED BY '{dbz_password}'")
    # conn.execute(f"GRANT {dbz_previllages} ON *.* TO '{dbz_user}'@'%'")

    print("done setting up debezium user")
    print("-" * 30)

    # conn.close()
    # engine.dispose()

    cursor.close()
    cnx.close()
