import json
from helpers.helper import ProcessingHelper


class BookingProcessor(ProcessingHelper):
    columns = ["id", "checkin", "checkout"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        payload["checkin"] = ProcessingHelper.to_date(payload["checkin"])
        payload["checkout"] = ProcessingHelper.to_date(payload["checkout"])
        self.upsert_to_db("stg_booking", payload, BookingProcessor.columns)
