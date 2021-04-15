import json,time,numpy as np
import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from google.oauth2 import service_account
#refresh button
st.button("Refresh")

# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)
# Create API client.
client = bigquery.Client(credentials=credentials)
query = "SELECT hourminute,StationName,Region, Technology, sum(SCADAVALUE) as Mw FROM `test-187010.ReportingDataset.today_Table`  group by 1,2,3,4"
result = pd.read_gbq(query, credentials=credentials)
# now have a DF result do stuff with it
column = result["hourminute"]
now = column.max()
st.text("Nem Power Generation as of")
st.write(now)
selection = alt.selection_multi(fields=['Technology'], bind='legend')
c = alt.Chart(result).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False)),
                                                    y='sum(Mw):Q',
                                                    color='Technology',
                                                    tooltip=['hourminute','Technology', 'Mw'],
                                                    opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
                                        ).properties(
                                            width=800,
                                            height=300
                                            ).add_selection(selection)
st.write(c)
st.write(result)
