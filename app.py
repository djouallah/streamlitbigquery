import json
import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from google.oauth2 import service_account
# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)
# Create API client.
client = bigquery.Client(credentials=credentials)
query = "SELECT hourminute, Region, sum(SCADAVALUE) as Mw FROM `test-187010.ReportingDataset.today_Table`  group by 1,2"
result = pd.read_gbq(query, credentials=credentials)
# now have a DF result do stuff with it
column = result["hourminute"]
now = column.max()
st.text("Nem Power Generation as of")
st.write(now)
c = alt.Chart(result).mark_area().encode(  x='hourminute', y='Mw',color='Region', tooltip=['hourminute','Region', 'Mw']).properties(
    width=800,
    height=300
)
st.write(c)
st.write(result)
