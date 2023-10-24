import streamlit as st
from constant import summary_by_gender, summary_by_age, summary_by_type, fetch_data
import plotly.express as px

date_format = "%Y%m%d000000"

st.set_page_config(layout="wide")

col1, col2 = st.columns([4, 1], gap="medium")
start_date = col2.date_input("Start Date", value=None)
end_date = col2.date_input("End Date", value=None)
col2.text("\n\n")
button = col2.button("Apply", use_container_width=True)

tab1, tab2 = col1.tabs(["Guest Attributes", "Guest Location"])
col31, col41 = tab1.columns(2)


def get_age_summary(start_datetime: str, end_datetime: str):
    query = summary_by_age.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(query)


def get_gender_summary(start_datetime: str, end_datetime: str):
    query = summary_by_gender.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(query)


def get_type_summary(start_datetime: str, end_datetime: str):
    query = summary_by_type.format(
        start_datetime=start_datetime, end_datetime=end_datetime
    )
    return fetch_data(query)


if button and start_date and end_date:
    start_datetime = start_date.strftime(date_format)
    end_datetime = end_date.strftime(date_format)
    age_summary = get_age_summary(start_datetime, end_datetime)
    col31.plotly_chart(
        px.bar(
            age_summary,
            x="age_range",
            y="revenue",
            hover_data=["revenue", "num_days"],
            # height=400,
            color="age_range",
            color_discrete_sequence=px.colors.sequential.Bluyl,
        ).update_layout(showlegend=False),
        use_container_width=True,
    )
    gender_summary = get_gender_summary(start_datetime, end_datetime)
    gender_pie = px.pie(
        gender_summary,
        values="revenue",
        names="gender",
        hole=0.5,
        custom_data="num_days",
        # height=360,
        color_discrete_sequence=px.colors.sequential.Bluyl,
    )
    gender_pie.update_traces(
        hovertemplate="gender:%{label} <br>revenue: %{value} <br>num_days: %{customdata}"
    )
    gender_pie.update_layout(legend_title="Gender")
    col41.plotly_chart(
        gender_pie,
        use_container_width=True,
    )
    # type_summary = get_type_summary(start_datetime, end_datetime)
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
