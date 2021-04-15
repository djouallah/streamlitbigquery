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


query = "SELECT SETTLEMENTDATE, Region, sum(SCADAVALUE) as Mw FROM `test-187010.ReportingDataset.today_Table`  group by 1,2"

result = pd.read_gbq(query, credentials=credentials)

column = result["SETTLEMENTDATE"]
now = column.max()
st.write(now)

c = alt.Chart(result).mark_area().encode(  x='SETTLEMENTDATE', y='Mw',color='Region', tooltip=['Region', 'Mw'])

st.write(c)
st.write(result)

