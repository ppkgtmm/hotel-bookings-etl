import json
from helpers.helper import ProcessingHelper


class GuestProcessor(ProcessingHelper):
    columns = ["id", "email", "dob", "gender"]
    stg_columns = columns + ["location"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]
        payload = payload.get("after")
        if not payload:
            return
        payload["dob"] = ProcessingHelper.to_date(payload["dob"])
        ProcessingHelper.upsert_to_db("stg_guest", payload, GuestProcessor.stg_columns)
        ProcessingHelper.upsert_to_db("dim_guest", payload, GuestProcessor.columns)
