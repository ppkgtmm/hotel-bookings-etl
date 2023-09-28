import pandas as pd
from common import TestHelper, booking_file, booking_room_file, booking_addon_file
from datetime import datetime

if __name__ == "__main__":
    bookings = pd.read_csv(booking_file)
    booking_rooms = pd.read_csv(booking_room_file)
    booking_addons = pd.read_csv(booking_addon_file)

    helper = TestHelper()

    for booking_room in booking_rooms.to_dict(orient="records"):
        booking = bookings[bookings.id == booking_room["booking"]]
        booking = booking.to_dict(orient="records")[0]
        fct_bookings = helper.get_fct_bookings(booking, booking_room)
        assert (
            len(fct_bookings)
            == (
                datetime.fromisoformat(booking["checkout"])
                - datetime.fromisoformat(booking["checkin"])
            ).days
            + 1
        )

    for booking_addon in booking_addons.to_dict(orient="records"):
        booking_room = booking_rooms[booking_rooms.id == booking_addon["booking_room"]]
        booking_room = booking_room.to_dict(orient="records")[0]
        fct_amenities = helper.get_fct_amenities(booking_room, booking_addon)
        assert len(fct_amenities) == 1

    helper.tear_down()
