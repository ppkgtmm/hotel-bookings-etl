import json
from helpers.processor import Processor


class RoomTypeProcessor(Processor):
    columns = ["_id", "name", "price", "created_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.roomtypes":
            return
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload.pop("created_at")
        payload["created_at"] = super().to_datetime(payload["updated_at"])
        super().insert_to_db("dim_roomtype", payload, RoomTypeProcessor.columns)
