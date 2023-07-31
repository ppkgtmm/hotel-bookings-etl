import json
from helpers.helper import ProcessingHelper


class RoomTypeProcessor(ProcessingHelper):
    columns = ["_id", "name", "price", "created_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload.pop("created_at")
        payload["created_at"] = ProcessingHelper.to_datetime(payload["updated_at"])
        self.insert_to_db("dim_roomtype", payload, RoomTypeProcessor.columns)
