from helpers.location import LocationProcessor
from helpers.guest import GuestProcessor
from helpers.addon import AddonProcessor
from helpers.room_type import RoomTypeProcessor
from helpers.room import RoomProcessor
from helpers.booking import BookingProcessor
from helpers.booking_room import BookingRoomProcessor
from helpers.booking_addon import BookingAddonProcessor
from helpers.helper import ProcessingHelper


class Processor(ProcessingHelper):
    def open(self, partition_id, epoch_id):
        super().open(partition_id, epoch_id)
        self.location = LocationProcessor()
        self.guest = GuestProcessor()
        self.room = RoomProcessor()
        self.addon = AddonProcessor()
        self.roomtype = RoomTypeProcessor()
        self.booking = BookingProcessor()
        self.booking_room = BookingRoomProcessor()
        self.booking_addon = BookingAddonProcessor()

    def process(self, row):
        topic = str(row.topic)
        if topic.endswith("location"):
            self.location.process(row)
        elif topic.endswith("guests"):
            self.guest.process(row)
        elif topic.endswith("addons"):
            self.addon.process(row)
        elif topic.endswith("roomtypes"):
            self.roomtype.process(row)
        elif topic.endswith("rooms"):
            self.room.process(row)
        elif topic.endswith("bookings"):
            self.booking.process(row)
        elif topic.endswith("booking_rooms"):
            self.booking_room.process(row)
        elif topic.endswith("booking_addons"):
            self.booking_addon.process(row)
        else:
            pass

    def close(self, error):
        return super().close(error)
