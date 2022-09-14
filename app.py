import json
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import altair as alt
from google.cloud import bigquery
from google.oauth2 import service_account
import duckdb 
from pathlib import Path

st.set_page_config(
    page_title="Australia Electricity Market Dashboard",
    page_icon="✅",
    layout="wide",
                  )

st.title("Australia Electricity Market Dashboard")
#refresh button
col1, col2 = st.columns([3, 1])

################################################### Download Data from BigQuery#####################################################
# Retrieve and convert key file content.
bigquery_key_json = json.loads(st.secrets["bigquery_key"], strict=False)
credentials = service_account.Credentials.from_service_account_info(bigquery_key_json)
# Create API client.
client = bigquery.Client(credentials=credentials)
query2 = '''--Streamlit 
        SELECT hourminute,StationName,Region, Technology, sum(SCADAVALUE) as Mw,min(RRP) as RRP
        FROM `test-187010.ReportingDataset.today_Table`  group by 1,2,3,4'''

# Query BgQuery
p = Path("data.parquet")
if not p.exists(): 
      pd.read_gbq(query2,credentials=credentials).to_parquet('data.parquet')
      st.write("Data loaded")

###################################################################################################################################

con = duckdb.connect()
StationName =con.execute('''select distinct StationName from read_parquet('data.parquet') order by StationName ;''').fetchdf()
option = st.multiselect('Select Stations',StationName['StationName'])
   
xxxx = "','".join(option)
        
select = (f'''('{xxxx}')''')
if len(option) == 0 : 
     Query = '''select * from read_parquet('data.parquet')  '''
else :
     Query = '''select * from read_parquet('data.parquet') where  StationName in '''+str(select)
        #st.write(Query)
con = duckdb.connect()
result =con.execute(Query).fetchdf()
con.close()
now = result["hourminute"].max()
        #st.dataframe(result)

st.subheader("Nem Power Generation Today: " + str(now))
        #st.write(now)
result2=result.groupby(['hourminute','StationName','Region'])['Mw'].sum().reset_index()
selection = alt.selection_multi(fields=['StationName'], bind='legend')
c = alt.Chart(result2).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='sum(Mw):Q',
                                                            color=alt.Color('StationName'),
                                                            
                                                            
                                                            
                                                            tooltip=['hourminute','StationName', 'Mw'],
                                                            
                                                ).properties(
                                                    width=350,
                                                    height=100
                                                    ).facet(
                                                    facet='StationName:N',
                                                columns=4
                                                                    ).resolve_scale(y='independent')

c2 = alt.Chart(result2).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='sum(Mw):Q',
                                                            color=alt.Color('Region'),
                                                            
                                                            
                                                            
                                                            tooltip=['hourminute','Region', 'Mw'],
                                                            
                                                ).properties(
                                                    width=350,
                                                    height=100
                                                    ).facet(
                                                    facet='Region:N',
                                                columns=4
                                                                    ).resolve_scale(y='independent')




result3=result.groupby(['hourminute','Region'])['RRP'].mean().reset_index()

c1 = alt.Chart(result3).mark_bar().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='RRP',
                                                            color=alt.condition(
                                                                        alt.datum.RRP > 0,
                                                                        alt.value("steelblue"),  # The positive color
                                                                        alt.value("red")),  # The negative color
                                                                        
                                                            tooltip=['hourminute','RRP']
                                                ).properties(
                                                    width=350,
                                                    height=100
                                                    ).facet(
                                                    facet='Region:N',
                                                        columns=4
                                                                ).resolve_scale(y='independent')
st.write(c1)
st.write(c2)
if len(option) != 0 : 
     st.write(c)

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


################## refresh data every 5 minutes 
st_autorefresh(interval=4 * 60 * 1000, key="dataframerefresh")
@st.experimental_memo(ttl=300)
def Get_Bq(query,_cred) :
        df=pd.read_gbq(query,credentials=_cred)
        df.to_parquet('data.parquet')


Get_Bq(query2,credentials)
