import json,time,numpy as np,base64
import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from google.oauth2 import service_account
#refresh button
col1, col2 = st.columns([3, 1])
col1.button("Refresh")



# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)
# Create API client.
client = bigquery.Client(credentials=credentials)
query = "SELECT hourminute,StationName,Region, Technology, sum(SCADAVALUE) as Mw,min(RRP) as RRP FROM `test-187010.ReportingDataset.today_Table`  group by 1,2,3,4"

# now have a DF result do stuff with it
result = pd.read_gbq(query,credentials=credentials)



column = result["hourminute"]
now = column.max()
st.subheader("Nem Power Generation Today: " + now)
#st.write(now)
result2=result.groupby(['hourminute','Technology','Region'])['Mw'].sum().reset_index()
selection = alt.selection_multi(fields=['Technology'], bind='legend')
c = alt.Chart(result2).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                    y='sum(Mw):Q',
                                                    color=alt.Color('Technology',
                                                    scale=alt.Scale(
                                                    domain=['Coal', 'Renewable','Rooftop','Gas','Fuel'],
                                                     range=['Black', 'green','red','yellow','gray'])),
                                                    tooltip=['hourminute','Technology', 'Mw'],
                                                    
                                        ).properties(
                                            width=400,
                                            height=100
                                            ).facet(
    facet='Region:N',
  columns=2
).resolve_scale(y='independent')
st.write(c)
result2=result.groupby(['hourminute','Region'])['RRP'].mean().reset_index()

c = alt.Chart(result2).mark_bar().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                    y='RRP',
                                                     color=alt.condition(
                                                                 alt.datum.RRP > 0,
                                                                 alt.value("steelblue"),  # The positive color
                                                                 alt.value("red")),  # The negative color
                                                                 
                                                    tooltip=['hourminute','RRP']
                                        ).properties(
                                            width=400,
                                            height=100
                                            ).facet(
    facet='Region:N',
  columns=2
).resolve_scale(y='independent')
st.write(c)
#st.write(result)

#download
def download_link(object_to_download, download_filename, download_link_text):
    """
    Generates a link to download the given object_to_download.

    object_to_download (str, pd.DataFrame):  The object to be downloaded.
    download_filename (str): filename and extension of file. e.g. mydata.csv, some_txt_output.txt
    download_link_text (str): Text to display for download link.

    Examples:
    download_link(YOUR_DF, 'YOUR_DF.csv', 'Click here to download data!')
    download_link(YOUR_STRING, 'YOUR_STRING.txt', 'Click here to download your text!')

    """
    if isinstance(object_to_download,pd.DataFrame):
        object_to_download = object_to_download.to_csv(index=False)

    # some strings <-> bytes conversions necessary here
    b64 = base64.b64encode(object_to_download.encode()).decode()

    return f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'


# Examples
tmp_download_link = download_link(result, 'YOUR_DF.csv', 'Export RAW Data')
col2.markdown(tmp_download_link, unsafe_allow_html=True)



