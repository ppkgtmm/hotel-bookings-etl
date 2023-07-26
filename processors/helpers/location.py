import json
from processor import Processor


class LocationProcessor(Processor):
    columns = ["id", "state", "country", "created_at", "updated_at"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.location":
            return
        payload = json.loads(row.value)["payload"]["after"]
        payload["created_at"] = super().to_timestamp(payload["created_at"])
        payload["updated_at"] = super().to_timestamp(payload["updated_at"])
