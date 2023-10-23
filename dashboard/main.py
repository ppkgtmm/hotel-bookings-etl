import streamlit as st

date_format = "%Y-%m-%d"

st.set_page_config(layout="wide")

col1, col2, col3, col4 = st.columns(4, gap="medium")
start_date = col2.date_input("start date", value=None)
end_date = col3.date_input("end date", value=None)
