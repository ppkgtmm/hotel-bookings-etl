import json
from helpers.processor import Processor


class AddonProcessor(Processor):
    columns = ["id", "name", "price"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        if row.topic != "oltp_hotel.oltp_hotel.addons":
            return
        payload = json.loads(row.value)["payload"]["after"]
        super().insert_to_db("dim_addon", payload)
