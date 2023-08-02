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
create_rooms_delta = """
CREATE TABLE IF NOT EXISTS delta.`/data/delta/rooms/` (
    id INT,
    type INT,
    updated_at TIMESTAMP  
) USING DELTA;
"""
create_guests_delta = """
CREATE TABLE IF NOT EXISTS delta.`/data/delta/guests/` (
    id INT,
    email STRING,
    dob DATE,
    gender STRING,
    location INT,
    updated_at TIMESTAMP  
) USING DELTA;
"""

create_bookings_delta = """
CREATE TABLE IF NOT EXISTS delta.`/data/delta/bookings/` (
    id INT,
    checkin DATE,
    checkout DATE 
) USING DELTA;
"""
create_booking_rooms_delta = """
CREATE TABLE IF NOT EXISTS delta.`/data/delta/booking_rooms/` (
    id INT,
    booking INT,
    room INT,
    guest INT,
    updated_at TIMESTAMP,
    processed BOOLEAN,
    modulus INT

) USING DELTA
PARTITIONED BY(modulus);
"""
create_booking_addons_delta = """
CREATE TABLE IF NOT EXISTS delta.`/data/delta/booking_addons/` (
    id INT,
    booking_room INT,
    addon INT,
    quantity INT,
    datetime TIMESTAMP,
    updated_at TIMESTAMP,
    processed BOOLEAN,
    date DATE

) USING DELTA
PARTITIONED BY(addon, date);
"""

create_delta_queries = [
    create_rooms_delta,
    create_guests_delta,
    create_bookings_delta,
    create_booking_rooms_delta,
    create_booking_addons_delta,
]


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


def write_locations(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_location (id, state, country)
                VALUES (:id, :state, :country)
                ON DUPLICATE KEY 
                UPDATE state=:state, country=:country
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_guests(row: Row):
    payload = row.asDict()
    query = """
                INSERT INTO dim_guest (id, email, dob, gender)
                VALUES (:id, :email, :dob, :gender)
                ON DUPLICATE KEY 
                UPDATE email=:email, dob=:dob, gender=:gender
            """
    conn.execute(text(query), payload)
    conn.commit()


def write_purchases(payload: Dict[str, Any]):
    query = """
                INSERT INTO fct_purchase (datetime, guest, guest_location, roomtype, addon, addon_quantity)
                VALUES (DATE_FORMAT(:datetime, '%Y%m%d%H%i%S'), :guest, :guest_location, :roomtype, :addon, :addon_quantity)
                ON DUPLICATE KEY 
                UPDATE addon_quantity = :addon_quantity
            """
    conn.execute(text(query), payload)
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
                "data.id",
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    data.foreach(write_addons)


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
                "data.id",
                "data.name",
                "data.price",
                timestamp_seconds(col("data.updated_at") / 1000).alias("updated_at"),
            ]
        )
    )
    data.foreach(write_roomtypes)


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
    data.foreach(write_locations)


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
    data.write.format("delta").mode("append").save("/data/delta/rooms/")


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
    data.select(["id", "email", "dob", "gender"]).foreach(write_guests)
    data.write.format("delta").mode("append").save("/data/delta/guests/")


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
    data.write.format("delta").mode("append").save("/data/delta/bookings/")


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
                lit(False).alias("processed"),
                (col("data.id") % 15).alias("modulus"),
            ]
        )
    )
    data.write.format("delta").mode("append").save("/data/delta/booking_rooms/")


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
                lit(False).alias("processed"),
                timestamp_seconds(col("data.datetime") / 1000)
                .cast("date")
                .alias("date"),
            ]
        )
    )
    data.write.format("delta").mode("append").save("/data/delta/booking_addons/")


def process_fct_purchase(micro_batch_df: DataFrame, batch_id: int):
    max_dt = micro_batch_df.select(max("updated_at").alias("max")).collect()[0].max
    micro_batch_df.sparkSession.read.format("jdbc").option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).option("url", jdbc_mysql_url).option("user", db_user).option(
        "password", db_password
    ).option(
        "query",
        f"""
            SELECT _id, MAX(id) AS latest_addon_id
            FROM dim_addon
            WHERE created_at <= CAST('{max_dt.strftime("%Y-%m-%d %H:%M:%S")}' AS DATETIME)
            GROUP BY 1
        """,
    ).load().createOrReplaceTempView(
        "dim_addon"
    )

    micro_batch_df.createOrReplaceTempView("booking_addons_stg")
    rows = micro_batch_df.sparkSession.sql(
        """
        SELECT
            ba.id,
            ba.datetime,
            br.guest AS guest,
            g.location AS guest_location,
            r.type AS roomtype,
            da.latest_addon_id addon,
            ba.quantity addon_quantity
        FROM booking_addons_stg ba
        INNER JOIN delta.`/data/delta/booking_rooms/` br
        ON ba.booking_room = br.id
        INNER JOIN delta.`/data/delta/guests/` g
        ON br.guest = g.id
        INNER JOIN delta.`/data/delta/rooms/` r
        ON br.room = r.id
        INNER JOIN dim_addon da
        ON ba.addon = da._id
        """
    ).collect()
    for row in rows:
        payload = row.asDict()
        booking_addon_id = payload.pop("id")
        write_purchases(payload)
        micro_batch_df.sparkSession.sql(
            """
            UPDATE delta.`/data/delta/booking_addons/`
            SET processed = true
            WHERE id = {id}
            """,
            id=booking_addon_id,
        )
