echo "downloading location data"
python3 generators/location.py
echo "downloaded location data"
echo "--------------------------------------"
echo "generating room types & addons data"
python3 generators/static.py
echo "generated room types & addons data"
echo "--------------------------------------"
echo "generating users & guests data"
python3 generators/person.py
echo "generated users & guests data"
echo "--------------------------------------"
echo "generating rooms data"
python3 generators/room.py
echo "generated rooms data"
echo "--------------------------------------"
echo "generating hotel bookings data"
python3 generators/booking.py
echo "generated hotel bookings data"
echo "--------------------------------------"
