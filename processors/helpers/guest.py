import json
from helpers.helper import ProcessingHelper


class GuestProcessor(ProcessingHelper):
    columns = ["id", "email", "dob", "gender"]
    stg_columns = columns + ["location", "updated_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value).get("payload", {})
        payload = payload.get("after")
        if not payload:
            return
        payload["dob"] = ProcessingHelper.to_date(payload["dob"])
        payload["updated_at"] = ProcessingHelper.to_datetime(payload["updated_at"])
        ProcessingHelper.insert_to_db(
            "stg_guest", payload, GuestProcessor.stg_columns, prepare=False
        )
        ProcessingHelper.upsert_to_db("dim_guest", payload, GuestProcessor.columns)
