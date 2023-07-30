import json
from helpers.processor import Processor


class BookingProcessor(Processor):
    columns = ["id", "checkin", "checkout"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload["checkin"] = Processor.to_date(payload["checkin"])
        payload["checkout"] = Processor.to_date(payload["checkout"])
        self.upsert_to_db("stg_booking", payload, BookingProcessor.columns)
