import json
from helpers.processor import Processor


class RoomProcessor(Processor):
    columns = ["id", "type"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        super().upsert_to_db("stg_room", payload, RoomProcessor.columns)
