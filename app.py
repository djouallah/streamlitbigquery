import json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)

# Create API client.
client = bigquery.Client(credentials=credentials)


query = "SELECT SETTLEMENTDATE, Region, sum(SCADAVALUE) as Mw FROM `test-187010.ReportingDataset.today_Table` where now=1 group by 1,2"
query_job = client.query(query)
rows = query_job.result()

# Print results.
for row in rows:
    st.write(f"{row.Region} is in :{row.Mw}:")
