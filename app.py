import json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)

# Create API client.
client = bigquery.Client(credentials=credentials)


query = "SELECT AirportID, Name, City, Country, IATA, ICAO FROM `testing-bi-engine.test.airport` LIMIT 1000"
query_job = client.query(query)
rows = query_job.result()

# Print results.
for row in rows:
    print("name={}, count={}".format(row[0], row["Country"]))
