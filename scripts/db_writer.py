from typing import Any, Dict
from sqlalchemy import create_engine, text

# , MetaData, Table
from sqlalchemy.pool import NullPool

# metadata = MetaData()


# stg_booking = Table(stg_booking_table, metadata, autoload_with=engine)
# del_booking = Table(stg_booking_table, metadata, autoload_with=engine)
# stg_booking_room = Table(stg_booking_table, metadata, autoload_with=engine)
# del_booking_room = Table(stg_booking_table, metadata, autoload_with=engine)
# stg_booking_addon = Table(stg_booking_table, metadata, autoload_with=engine)
# del_booking_addon = Table(stg_booking_table, metadata, autoload_with=engine)


def execute_query(connection_string: str, query: str):
    engine = create_engine(connection_string, poolclass=NullPool)
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
