import json
from helpers.processor import Processor
from sqlalchemy import text
from datetime import timedelta


class BookingRoomProcessor(Processor):
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
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload["updated_at"] = super().to_datetime(payload["updated_at"])
        super().upsert_to_db("stg_booking_room", payload, BookingRoomProcessor.columns)
        guest = Processor.conn.execute(
            BookingRoomProcessor.guest_q, {"id": payload["guest"]}
        ).first()
        room = Processor.conn.execute(
            BookingRoomProcessor.room_q, {"id": payload["room"]}
        ).first()
        room_type = Processor.conn.execute(
            BookingRoomProcessor.roomtype_q,
            {"_id": room[0], "created_at": payload["updated_at"]},
        ).first()
        booking = Processor.conn.execute(
            BookingRoomProcessor.booking_q, {"id": payload["booking"]}
        ).first()
        current_date, end_date = booking[0], booking[1]
        while current_date <= end_date:
            data = {
                "guest": payload["guest"],
                "guest_location": guest[0],
                "roomtype": room_type[0],
                "datetime": int(current_date.strftime("%Y%m%d%H%M%S")),
            }
            super().upsert_to_db("fct_booking", data, BookingRoomProcessor.fct_columns)
            current_date += timedelta(days=1)
