import streamlit as st
import plotly.express as px
from datetime import datetime
from dateutil.relativedelta import relativedelta

date_format = "DD/MM/YYYY"
col1, col2 = st.columns(2, gap="medium")
start_date = col1.date_input(
    "Start Date", value=datetime.today().replace(day=1), format=date_format
)
end_date = col2.date_input(
    "End Date",
    value=datetime.today() + relativedelta(months=1, day=1, days=-1),
    format=date_format,
)
