import json
from helpers.helper import ProcessingHelper
from sqlalchemy import text
from datetime import timedelta


class BookingRoomProcessor(ProcessingHelper):
    columns = ["id", "booking", "room", "guest", "updated_at"]
    fct_columns = ["datetime", "guest", "guest_location", "roomtype"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value).get("payload", {})
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
                    br.booking,
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
                ON br.processed = false AND br.booking = b.id
                INNER JOIN (
                    SELECT id, location, updated_at, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnk
                    FROM stg_guest
                    WHERE updated_at <= :updated_at
                ) g
                ON br.guest = g.id AND g.rnk = 1
                INNER JOIN (
                    SELECT id, type, updated_at,  ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnk
                    FROM stg_room
                    WHERE updated_at <= :updated_at
                ) r
                ON br.room = r.id AND r.rnk = 1
                """,
            ),
            {
                "updated_at": payload["updated_at"],
            },
        )
        for row in data:
            (
                id,
                booking,
                checkin,
                checkout,
                guest,
                guest_location,
                g_updated_at,
                room,
                r_updated_at,
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
                ProcessingHelper.upsert_to_db(
                    "fct_booking", data, BookingRoomProcessor.fct_columns
                )
                current_date += timedelta(days=1)
            ProcessingHelper.conn.execute(
                text(
                    "DELETE FROM stg_room WHERE id = :id AND updated_at < :updated_at"
                ),
                {"id": room, "updated_at": r_updated_at},
            )
            ProcessingHelper.conn.commit()
            ProcessingHelper.conn.execute(
                text(
                    "DELETE FROM stg_guest WHERE id = :id AND updated_at < :updated_at"
                ),
                {"id": guest, "updated_at": g_updated_at},
            )
            ProcessingHelper.conn.commit()
