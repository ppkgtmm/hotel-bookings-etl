from dotenv import load_dotenv
from os import getenv
import sys
from os.path import abspath, dirname, join

sys.path.append(dirname(dirname(abspath(__file__))))
from processors.constants import fct_booking_table, fct_purchase_table, dim_addon_table

dt_fmt = "%Y%m%d%H%M%S"
result_folder = join(dirname(abspath(__file__)), "results")

load_dotenv()

db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
oltp_db = getenv("OLTP_DB")
olap_db = getenv("OLAP_DB")
location_table = getenv("LOCATION_TABLE")
guests_table = getenv("GUESTS_TABLE")
addons_table = getenv("ADDONS_TABLE")
roomtypes_table = getenv("ROOMTYPES_TABLE")
rooms_table = getenv("ROOMS_TABLE")
bookings_table = getenv("BOOKINGS_TABLE")
booking_rooms_table = getenv("BOOKING_ROOMS_TABLE")
booking_addons_table = getenv("BOOKING_ADDONS_TABLE")

conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}"
oltp_conn_str = conn_str.format(db_user, db_password, db_host, db_port, oltp_db)
olap_conn_str = conn_str.format(db_user, db_password, db_host, db_port, olap_db)
