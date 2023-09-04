from dotenv import load_dotenv
import os
import requests

load_dotenv()

DB_HOST_INTERNAL = os.getenv("DB_HOST_INTERNAL")
DBZ_USER = os.getenv("DBZ_USER")
DBZ_PASSWORD = os.getenv("DBZ_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
OLTP_DB = os.getenv("OLTP_DB")
DBZ_CONNECTOR = os.getenv("DBZ_CONNECTOR")
KSQL_LISTENERS = os.getenv("KSQL_LISTENERS")
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL")

if __name__ == "__main__":
    query_path = f"{os.path.dirname(__file__)}/ksql/register_source.sql"
    with open(query_path, "r") as fp:
        query = fp.read()
    query = (
        query.replace("${DBZ_CONNECTOR}", DBZ_CONNECTOR)
        .replace("${DB_HOST_INTERNAL}", DB_HOST_INTERNAL)
        .replace("${DBZ_USER}", DBZ_USER)
        .replace("${DBZ_PASSWORD}", DBZ_PASSWORD)
        .replace("${DB_PORT}", DB_PORT)
        .replace("${OLTP_DB}", OLTP_DB)
        .replace(
            "${KAFKA_BOOTSTRAP_SERVERS_INTERNAL}", KAFKA_BOOTSTRAP_SERVERS_INTERNAL
        )
    )
    response = requests.post(
        f"{KSQL_LISTENERS}/ksql/",
        headers={
            "Accept": "application/vnd.ksql.v1+json",
            "Content-Type": "application/json",
        },
        json={"ksql": query},
    )
    assert response.status_code < 400
    for r in response.json():
        assert r.get("error_code") is None
