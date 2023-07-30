import json
from helpers.processor import Processor


class GuestProcessor(Processor):
    columns = ["id", "email", "dob", "gender"]
    stg_columns = columns + ["location"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload["dob"] = Processor.to_date(payload["dob"])
        self.upsert_to_db("stg_guest", payload, GuestProcessor.stg_columns)
        self.upsert_to_db("dim_guest", payload, GuestProcessor.columns)
