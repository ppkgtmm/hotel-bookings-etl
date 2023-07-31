import json
from helpers.helper import ProcessingHelper


class LocationProcessor(ProcessingHelper):
    columns = ["id", "state", "country"]

    def __init__(self):
        super().__init__()

    def process(self, row):
        payload = json.loads(row.value)["payload"]["after"]
        if not payload:
            return
        self.upsert_to_db("dim_location", payload, LocationProcessor.columns)
