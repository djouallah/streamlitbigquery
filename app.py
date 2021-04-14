import json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)

# Create API client.
client = bigquery.Client(credentials=credentials)


query = """
    SELECT * FROM test-187010.ReportingDataset.today_Table  where now =1
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))
