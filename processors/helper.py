from typing import Any, Dict
from dotenv import load_dotenv
from os import getenv
from pyspark.sql.types import StringType, MapType
from sqlalchemy import create_engine, text
from pyspark.sql.types import (
    IntegerType,
    LongType,
    FloatType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import (
    from_json,
    col,
    timestamp_seconds,
    date_add,
    to_date,
    lit,
    max,
)
from datetime import timedelta

load_dotenv()
db_host = getenv("DB_HOST_INTERNAL")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("OLAP_DB")

connection_string = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    db_user, db_password, db_host, db_port, db_name
)
engine = create_engine(connection_string)
conn = engine.connect()
jdbc_mysql_url = f"jdbc:mysql://{db_host}:{db_port}/{db_name}"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
stg_location_table = "stg_location"
stg_room_table = "stg_room"
stg_guest_table = "stg_guest"
stg_booking_table = "stg_booking"
stg_booking_room_table = "stg_booking_room"
stg_booking_addon_table = "stg_booking_addon"
addon_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", FloatType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
roomtype_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", FloatType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
location_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
room_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("floor", IntegerType()),
        StructField("number", IntegerType()),
        StructField("type", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
guest_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("gender", StringType()),
        StructField("email", StringType()),
        StructField("dob", IntegerType()),
        StructField("location", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("user", IntegerType()),
        StructField("checkin", IntegerType()),
        StructField("checkout", IntegerType()),
        StructField("payment", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_room_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking", IntegerType()),
        StructField("room", IntegerType()),
        StructField("guest", IntegerType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
booking_addon_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("booking_room", IntegerType()),
        StructField("addon", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("datetime", LongType()),
        StructField("created_at", LongType()),
        StructField("updated_at", LongType()),
    ]
)
json_schema = MapType(StringType(), StringType())


def write_addons(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_addon (_id, name, price, created_at)
                VALUES (:id, :name, :price, :updated_at)
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_roomtypes(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_roomtype (_id, name, price, created_at)
                VALUES (:id, :name, :price, :updated_at)
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_locations():
    query = f"""
                INSERT INTO dim_location (id, state, country)
                SELECT id, state, country
                FROM {stg_location_table} stg
                ON DUPLICATE KEY 
                UPDATE state=stg.state, country=stg.country
            """
    conn.execute(text(query))
    conn.commit()


def write_guests():
    query = f"""
                INSERT INTO dim_guest (id, email, dob, gender)
                SELECT id, email, dob, gender
                FROM {stg_guest_table} stg
                ON DUPLICATE KEY 
                UPDATE email=stg.email, dob=stg.dob, gender=stg.gender
            """
    conn.execute(text(query))
    conn.commit()


def process_addons(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", addon_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                col("data.id").alias("_id"),
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("created_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", "dim_addon")
        .mode("append")
        .save()
    )


def process_roomtypes(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", roomtype_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                col("data.id").alias("_id"),
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("created_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", "dim_roomtype")
        .mode("append")
        .save()
    )


def process_locations(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", location_schema))
        .filter("data IS NOT NULL")
        .select(["data.id", "data.state", "data.country"])
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_location_table)
        .mode("overwrite")
        .save()
    )
    write_locations()


def process_rooms(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", room_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.type",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_room_table)
        .mode("append")
        .save()
    )


def process_guests(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", guest_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.email",
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.dob")
                ).alias("dob"),
                "data.gender",
                "data.location",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_guest_table)
        .mode("append")
        .save()
    )
    write_guests()


def process_bookings(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", booking_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkin")
                ).alias("checkin"),
                date_add(
                    to_date(lit("1970-01-01"), "yyyy-MM-dd"), col("data.checkout")
                ).alias("checkout"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_booking_table)
        .mode("append")
        .save()
    )


def process_booking_rooms(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", booking_room_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.booking",
                "data.room",
                "data.guest",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_booking_room_table)
        .mode("append")
        .save()
    )
    write_bookings()


def write_bookings():
    query = f"""
    WITH max_date AS (
        SELECT MAX(updated_at)
        FROM {stg_booking_room_table}
    )
    SELECT
        br.id,
        b.checkin, 
        b.checkout, 
        br.guest, 
        g.location guest_location,
        (
            SELECT MAX(id) id
            FROM dim_roomtype
            WHERE _id = r.type AND created_at <= br.updated_at
        ) room_type
    FROM {stg_booking_room_table} br
    INNER JOIN {stg_booking_table} b
    ON br.processed = false AND br.booking = b.id
    INNER JOIN (
        SELECT g.id, location, updated_at, ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY updated_at DESC) rnk
        FROM {stg_guest_table} g
        INNER JOIN dim_location l
        ON g.location = l.id
        WHERE updated_at <= (SELECT * FROM max_date)
    ) g
    ON br.guest = g.id AND g.rnk = 1
    INNER JOIN (
        SELECT id, type, updated_at,  ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnk
        FROM {stg_room_table}
        WHERE updated_at <= (SELECT * FROM max_date)
    ) r
    ON br.room = r.id AND r.rnk = 1
    """
    for row in conn.execute(text(query)):
        (
            id,
            checkin,
            checkout,
            guest,
            guest_location,
            room_type,
        ) = row
        current_date = checkin
        while current_date <= checkout:
            data = {
                "guest": guest,
                "guest_location": guest_location,
                "roomtype": room_type,
                "datetime": int(current_date.strftime("%Y%m%d%H%M%S")),
            }
            conn.execute(
                text(
                    """
                    INSERT INTO fct_booking (datetime, guest, guest_location, roomtype)
                    VALUES (:datetime, :guest, :guest_location, :roomtype)
                    ON DUPLICATE KEY
                    UPDATE datetime=:datetime, guest=:guest, guest_location=:guest_location, roomtype=:roomtype
                    """
                ),
                data,
            )
            conn.commit()
            conn.execute(
                text(
                    f"UPDATE {stg_booking_room_table} SET processed = true WHERE id = :id"
                ),
                {"id": id},
            )
            conn.commit()
            current_date += timedelta(days=1)


def process_booking_addons(micro_batch_df: DataFrame, batch_id: int):
    data: DataFrame = (
        micro_batch_df.withColumn(
            "message", from_json(col("value").cast(StringType()), json_schema)
        )
        .withColumn("payload", from_json("message.payload", json_schema))
        .withColumn("data", from_json("payload.after", booking_addon_schema))
        .filter("data IS NOT NULL")
        .select(
            [
                "data.id",
                "data.booking_room",
                "data.addon",
                "data.quantity",
                timestamp_seconds(col("data.datetime") / 1000).alias("datetime"),
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    (
        data.write.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_mysql_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", stg_booking_addon_table)
        .mode("append")
        .save()
    )
    write_purchases()


def write_purchases():
    query = f"""
    WITH max_date AS (
        SELECT MAX(updated_at)
        FROM {stg_booking_addon_table}
    )
    SELECT
        ba.id,
        ba.datetime, 
        br.guest, 
        g.location guest_location,
        (
            SELECT MAX(id) id
            FROM dim_roomtype
            WHERE _id = r.type AND created_at <= ba.updated_at
        ) room_type,
        (
            SELECT MAX(id) id
            FROM dim_addon
            WHERE _id = ba.addon AND created_at <= ba.updated_at
        ) addon,
        ba.quantity
    FROM {stg_booking_addon_table} ba
    INNER JOIN {stg_booking_room_table} br
    ON ba.processed = false AND ba.booking_room = br.id
    INNER JOIN (
        SELECT g.id, location, updated_at, ROW_NUMBER() OVER(PARTITION BY g.id ORDER BY updated_at DESC) rnk
        FROM {stg_guest_table} g
        INNER JOIN dim_location l
        ON g.location = l.id
        WHERE updated_at <= (SELECT * FROM max_date)
    ) g
    ON br.guest = g.id AND g.rnk = 1
    INNER JOIN (
        SELECT id, type, updated_at,  ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnk
        FROM {stg_room_table}
        WHERE updated_at <= (SELECT * FROM max_date)
    ) r
    ON br.room = r.id AND r.rnk = 1
    """
    for row in conn.execute(text(query)):
        (id, datetime, guest, guest_location, room_type, addon, quantity) = row
        data = {
            "guest": guest,
            "guest_location": guest_location,
            "roomtype": room_type,
            "datetime": int(datetime.strftime("%Y%m%d%H%M%S")),
            "addon": addon,
            "addon_quantity": quantity,
        }
        conn.execute(
            text(
                """
                    INSERT INTO fct_purchase (datetime, guest, guest_location, roomtype, addon, addon_quantity)
                    VALUES (:datetime, :guest, :guest_location, :roomtype, :addon, :addon_quantity)
                    ON DUPLICATE KEY
                    UPDATE addon_quantity=:addon_quantity
                    """
            ),
            data,
        )
        conn.commit()
        conn.execute(
            text(
                f"UPDATE {stg_booking_addon_table} SET processed = true WHERE id = :id"
            ),
            {"id": id},
        )
        conn.commit()


# def process_fct_purchase(micro_batch_df: DataFrame):
#     max_dt = micro_batch_df.select(max("updated_at").alias("max")).collect()[0].max
#     micro_batch_df.sparkSession.read.format("jdbc").option(
#         "driver", "com.mysql.cj.jdbc.Driver"
#     ).option("url", jdbc_mysql_url).option("user", db_user).option(
#         "password", db_password
#     ).option(
#         "query",
#         f"""
#             SELECT _id, MAX(id) AS latest_addon_id
#             FROM dim_addon
#             WHERE created_at <= CAST('{max_dt.strftime("%Y-%m-%d %H:%M:%S")}' AS DATETIME)
#             GROUP BY 1
#         """,
#     ).load().createOrReplaceTempView(
#         "dim_addon"
#     )
#     micro_batch_df.createOrReplaceTempView("booking_addons")
#     rows = micro_batch_df.sparkSession.sql(
#         """
#         SELECT
#             ba.id,
#             ba.datetime,
#             br.guest AS guest,
#             g.location AS guest_location,
#             r.type AS roomtype,
#             da.latest_addon_id addon,
#             ba.quantity addon_quantity
#         FROM booking_addons ba
#         INNER JOIN delta.`/data/delta/booking_rooms/` br
#         ON ba.booking_room = br.id
#         INNER JOIN delta.`/data/delta/guests/` g
#         ON br.guest = g.id
#         INNER JOIN delta.`/data/delta/rooms/` r
#         ON br.room = r.id
#         INNER JOIN dim_addon da
#         ON ba.addon = da._id
#         ORDER BY ba.id
#         """
#     ).collect()


#     for row in rows:
#         payload = row.asDict()
#         booking_addon_id = payload.pop("id")
#         write_purchases(payload)
#         micro_batch_df.sparkSession.sql(
#             """
#             UPDATE delta.`/data/delta/booking_addons/`
#             SET processed = true
#             WHERE id = {id} AND partition_num = MOD({id}, 15) AND date = DATE('{datetime}')
#             """,
#             id=booking_addon_id,
#             datetime=payload["datetime"],
#         )
