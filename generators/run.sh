echo "downloading location data"
python3 generators/location.py
echo "downloaded location data"
echo "generating room types & addons data"
python3 generators/static.py
echo "generated room types & addons data"
echo "generating users & guests data"
python3 generators/person.py
echo "generated users & guests data"
echo "generating rooms data"
python3 generators/room.py
echo "generated rooms data"
echo "generating hotel bookings data"
python3 generators/booking.py
echo "generated hotel bookings data"
