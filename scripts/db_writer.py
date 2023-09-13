from typing import Any, Dict
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def execute_query(connection_string: str, query: str, data: list[Dict[str, Any]]):
    engine = create_engine(connection_string, poolclass=NullPool)
    with engine.connect() as conn:
        conn.execute(query, data)
        conn.commit()
