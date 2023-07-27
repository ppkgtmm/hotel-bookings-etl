import json
from helpers.processor import Processor


class AddonProcessor(Processor):
    columns = ["_id", "name", "price"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.addons":
            return
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        super().insert_to_db("dim_addon", payload)
