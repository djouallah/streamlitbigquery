import json,time,numpy as np,base64
import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Example of BigQuery and Streamlit",
    page_icon="✅",
    layout="wide",
)

st.title("Example of using BigQuery and Streamlit")
#refresh button
col1, col2 = st.columns([3, 1])
col1.button("Refresh")



# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)
# Create API client.
client = bigquery.Client(credentials=credentials)
query = '''--Streamlit 
        SELECT hourminute,StationName,Region, Technology, sum(SCADAVALUE) as Mw,min(RRP) as RRP
        FROM `test-187010.ReportingDataset.today_Table`  group by 1,2,3,4'''

# now have a DF result do stuff with it
@st.experimental_memo
  Def Get_Bq(query,_cred) :
        df=pd.read_gbq(query,credentials=_cred)
        return df

result = Get_Bq(query,credentials)

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
result3=result.groupby(['hourminute','Region'])['RRP'].mean().reset_index()

c = alt.Chart(result3).mark_bar().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
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
#st.table(result2)

#Download Button


def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(result)
col2.download_button(
     label="Download data as CSV",
     data=csv,
     file_name='large_df.csv',
     mime='text/csv',
 )

link='[Blog](https://datamonkeysite.com/category/streamlit/)'
col1.markdown(link,unsafe_allow_html=True)

