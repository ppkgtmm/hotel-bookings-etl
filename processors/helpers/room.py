import json
from helpers.helper import ProcessingHelper


class RoomProcessor(ProcessingHelper):
    columns = ["id", "type"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]
        payload = payload.get("after")
        if not payload:
            return
        ProcessingHelper.upsert_to_db("stg_room", payload, RoomProcessor.columns)
