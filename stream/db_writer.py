from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


def execute_query(connection_string: str, query: str):
    engine = create_engine(connection_string, poolclass=NullPool)
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
