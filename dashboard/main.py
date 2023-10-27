from urllib.request import urlopen
import streamlit as st
from dotenv import load_dotenv
from os import getenv
from query import *
import plotly.express as px
import json

date_format = "%Y%m%d000000"
load_dotenv()
db_host = getenv("DB_HOST")
db_port = getenv("DB_PORT")
db_user = getenv("DB_USER")
db_password = getenv("DB_PASSWORD")
db_name = getenv("DWH_DB")
json_url = getenv("GEO_JSON_FILE")

connection_string = (
    f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# col4.plotly_chart(
#     px.bar(
#         type_summary,
#         y="name",
#         x="revenue",
#         hover_data=["revenue", "num_days"],
#         height=320,
#         color="name",
#         color_discrete_sequence=px.colors.sequential.Bluyl_r,
#     ).update_layout(showlegend=False),
#     use_container_width=True,
# )

st.set_page_config(layout="wide")


@st.cache_data
def get_age_summary(start_datetime: str, end_datetime: str):
    query = summary_by_age.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(connection_string, query)


@st.cache_data
def get_gender_summary(start_datetime: str, end_datetime: str):
    query = summary_by_gender.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(connection_string, query)


@st.cache_data
def get_type_summary(start_datetime: str, end_datetime: str):
    query = summary_by_type.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(connection_string, query)


@st.cache_data
def get_location_summary(start_datetime: str, end_datetime: str):
    query = summary_by_location.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(connection_string, query)


@st.cache_resource
def get_geo_json(url: str):
    with urlopen(url) as response:
        return json.load(response)


geo_json = get_geo_json(json_url)

col1, col2 = st.columns([4, 1], gap="medium")
start_date = col2.date_input("Start Date", value=None)
end_date = col2.date_input("End Date", value=None)
col2.text("\n\n")
button = col2.button("Apply", use_container_width=True)

tab1, tab2 = col1.tabs(["Guest Attributes", "Guest Location"])
col11, col12 = tab1.columns(2)

if button and start_date and end_date:
    start_datetime = start_date.strftime(date_format)
    end_datetime = end_date.strftime(date_format)
    query_args = [start_datetime, end_datetime]
    col11.plotly_chart(
        px.bar(
            get_age_summary(*query_args),
            x="age_range",
            y="revenue",
            hover_data=["revenue", "num_days"],
            color="age_range",
            color_discrete_sequence=px.colors.sequential.Bluyl,
        ).update_layout(showlegend=False),
        use_container_width=True,
    )
    col12.plotly_chart(
        px.pie(
            get_gender_summary(*query_args),
            values="revenue",
            names="gender",
            hole=0.6,
            custom_data="num_days",
            color_discrete_sequence=px.colors.sequential.Bluyl,
        )
        .update_traces(
            hovertemplate="gender:%{label} <br>revenue: %{value} <br>num_days: %{customdata}"
        )
        .update_layout(legend_title="Gender"),
        use_container_width=True,
    )
    tab2.plotly_chart(
        px.choropleth(
            get_location_summary(*query_args),
            locations="fips",
            geojson=geo_json,
            color="revenue",
            featureidkey="properties.fips",
            color_continuous_scale=px.colors.sequential.Bluyl,
            template="plotly_dark",
            fitbounds="locations",
            hover_data=["state", "country", "num_days", "revenue"],
        ),
        use_container_width=True,
    )
# gender_summary = get_gender_summary(start_datetime, end_datetime)
# type_summary = get_type_summary(start_datetime, end_datetime)
