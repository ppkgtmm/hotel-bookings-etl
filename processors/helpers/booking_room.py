import json
from helpers.helper import ProcessingHelper
from sqlalchemy import text
from datetime import timedelta


class BookingRoomProcessor(ProcessingHelper):
    columns = ["id", "booking", "room", "guest", "updated_at"]
    fct_columns = ["datetime", "guest", "guest_location", "roomtype"]
    guest_q = text("SELECT location FROM stg_guest WHERE id = :id")
    booking_q = text("SELECT checkin, checkout FROM stg_booking WHERE id = :id")
    room_q = text("SELECT type FROM stg_room WHERE id = :id")
    roomtype_q = text(
        "SELECT max(id) FROM dim_roomtype WHERE _id = :_id AND created_at <= :created_at"
    )

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value).get(["payload"], {})
        payload = payload.get("after")
        if not payload:
            return
        payload["updated_at"] = ProcessingHelper.to_datetime(payload["updated_at"])
        ProcessingHelper.upsert_to_db(
            "stg_booking_room", payload, BookingRoomProcessor.columns
        )
        data = ProcessingHelper.conn.execute(
            text(
                """
                SELECT 
                    br.id, 
                    b.checkin, 
                    b.checkout, 
                    br.guest, 
                    g.location guest_location, 
                    g.updated_at g_updated_at, 
                    br.room,
                    r.updated_at r_updated_at,
                    (
                        SELECT MAX(id) id
                        FROM dim_roomtype
                        WHERE _id = r.type AND created_at <= br.updated_at
                    ) room_type
                FROM stg_booking_room br
                INNER JOIN stg_booking b
                ON br.booking = b.id
                INNER JOIN (
                    SELECT id, location, updated_at, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rank
                    FROM stg_guest
                    WHERE updated_at <= :updated_at
                ) g
                ON br.guest = g.id AND rank = 1
                INNER JOIN (
                    SELECT id, type, updated_at,  ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rank
                    FROM stg_room
                    WHERE updated_at <= :updated_at
                ) r
                ON br.room = r.id AND rank = 1
                """,
                {"updated_at": payload["updated_at"]},
            )
        )
        for row in data:
            pass
        # while current_date <= end_date:
        #     data = {
        #         "guest": payload["guest"],
        #         "guest_location": guest[0],
        #         "roomtype": room_type[0],
        #         "datetime": int(current_date.strftime("%Y%m%d%H%M%S")),
        #     }
        #     ProcessingHelper.upsert_to_db(
        #         "fct_booking", data, BookingRoomProcessor.fct_columns
        #     )
        #     current_date += timedelta(days=1)
