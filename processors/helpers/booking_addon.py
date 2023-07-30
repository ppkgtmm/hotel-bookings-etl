import json
from helpers.processor import Processor
from sqlalchemy import text


class BookingAddonProcessor(Processor):
    columns = ["id", "booking_room", "addon", "quantity", "datetime", "updated_at"]
    fct_columns = [
        "datetime",
        "guest",
        "guest_location",
        "roomtype",
        "addon",
        "addon_quantity",
    ]
    addon_q = text(
        "SELECT max(id) FROM dim_addon WHERE _id = :_id AND created_at <= :created_at"
    )
    booking_room_q = text("SELECT guest, room FROM stg_booking_room WHERE id = :id")
    guest_q = text("SELECT location FROM stg_guest WHERE id = :id")
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
        payload["datetime"] = super().to_datetime(payload["datetime"])
        super().upsert_to_db(
            "stg_booking_addon", payload, BookingAddonProcessor.columns
        )
        addon = Processor.conn.execute(
            BookingAddonProcessor.addon_q,
            {"_id": payload["addon"], "created_at": payload["updated_at"]},
        ).first()

        booking_room = Processor.conn.execute(
            BookingAddonProcessor.booking_room_q, {"id": payload["booking_room"]}
        ).first()
        guest, room = booking_room[0], booking_room[1]
        guest_location = Processor.conn.execute(
            BookingAddonProcessor.guest_q, {"id": guest}
        ).first()
        room_stg = Processor.conn.execute(
            BookingAddonProcessor.room_q, {"id": room}
        ).first()
        room_type = Processor.conn.execute(
            BookingAddonProcessor.roomtype_q,
            {"_id": room_stg[0], "created_at": payload["updated_at"]},
        ).first()
        data = {
            "guest": guest,
            "guest_location": guest_location[0],
            "roomtype": room_type[0],
            "datetime": int(payload["datetime"].strftime("%Y%m%d%H%M%S")),
            "addon": addon[0],
            "addon_quantity": payload["quantity"],
        }
        super().upsert_to_db("fct_purchase", data, BookingAddonProcessor.fct_columns)
