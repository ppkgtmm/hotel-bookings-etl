import json
from helpers.helper import ProcessingHelper


class RoomProcessor(ProcessingHelper):
    columns = ["id", "type", "updated_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value).get("payload", {})
        payload = payload.get("after")
        if not payload:
            return
        payload["updated_at"] = ProcessingHelper.to_datetime(payload["updated_at"])
        ProcessingHelper.insert_to_db("stg_room", payload, RoomProcessor.columns, False)
