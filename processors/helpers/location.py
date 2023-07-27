import json
from helpers.processor import Processor


class LocationProcessor(Processor):
    columns = ["id", "state", "country"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.location":
            return
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        super().upsert_to_db("dim_location", payload)
