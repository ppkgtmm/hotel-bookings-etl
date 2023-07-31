import json
from helpers.helper import ProcessingHelper


class AddonProcessor(ProcessingHelper):
    columns = ["_id", "name", "price", "created_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]
        payload = payload.get("after")
        if not payload:
            return
        payload.pop("created_at")
        payload["created_at"] = super().to_datetime(payload["updated_at"])
        ProcessingHelper.insert_to_db("dim_addon", payload, AddonProcessor.columns)
