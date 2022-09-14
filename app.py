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
        SELECT hourminute,StationName,Region, Technology,SETTLEMENTDATE, sum(SCADAVALUE) as Mw,min(RRP) as RRP
        FROM `test-187010.ReportingDataset.today_Table` where rrp is not null and SCADAVALUE != 0 group by 1,2,3,4,5'''

# Query BgQuery
p = Path("data.parquet")
if not p.exists(): 
      pd.read_gbq(query2,credentials=credentials).to_parquet('data.parquet')
      st.write("Data loaded")

###################################################################################################################################

con = duckdb.connect()
con.execute('''create or replace view source as select * from read_parquet('data.parquet')''')
StationName =con.execute('''select distinct StationName from source order by StationName ;''').fetchdf()
option = st.multiselect('Select Stations',StationName['StationName'])
   
xxxx = "','".join(option)
        
select = (f'''('{xxxx}')''')
if len(option) == 0 : 
     result =con.execute('''select * from source ''').df()
else :
     result =con.execute('select * from source where  StationName in '+str(select)).df()
        #st.write(Query)
now = con.execute('''select strftime(max(SETTLEMENTDATE), '%A, %-d %B %Y - %I:%M:%S %p') as SETTLEMENTDATE  from result''').df()
now = now[['SETTLEMENTDATE']].values[0][0]

st.subheader( str(now))
result1=  con.execute('select hourminute,StationName,Region,Technology, sum (Mw) as Mw from result group by all').df()
selection = alt.selection_multi(fields=['StationName'], bind='legend')
c = alt.Chart(result1).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='sum(Mw):Q',
                                                            color=alt.Color('Technology'),
                                                            tooltip=['hourminute','StationName', 'Mw'],
                                                            
                                                ).properties(
                                                    width=250,
                                                    height=100
                                                    ).facet(
                                                    facet='StationName:N',
                                                    columns=5
                                                                    ).resolve_scale(y='independent')

result2=con.execute('select hourminute,Region,Technology, sum (Mw) as Mw from result group by all').df()
c2 = alt.Chart(result2).mark_area().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='sum(Mw):Q',
                                                            color=alt.Color('Technology'),
                                                            tooltip=['hourminute','Region', 'Mw'],
                                                            
                                                ).properties(
                                                    width=250,
                                                    height=100
                                                    ).facet(
                                                    facet='Region:N',
                                                    columns=5
                                                                    ).resolve_scale(y='independent')


result3=con.execute('select hourminute,Region, Min (RRP) as RRP from result group by all').df()
c1 = alt.Chart(result3).mark_bar().encode(  x=alt.X('hourminute:O',axis=alt.Axis(labels=False,ticks=False)),
                                                            y='RRP',
                                                            color=alt.condition(
                                                                        alt.datum.RRP > 0,
                                                                        alt.value("steelblue"),  # The positive color
                                                                        alt.value("red")),  # The negative color
                                                                        
                                                            tooltip=['hourminute','RRP']
                                                ).properties(
                                                    width=250,
                                                    height=100
                                                    ).facet(
                                                    facet='Region:N',
                                                        columns=5
                                                                ).resolve_scale(y='independent')

if len(option) != 0 : 
     st.write(c1)
     st.write(c)
else :
    st.write(c1)
    st.write(c2)
    
del result1,result2,result3
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
