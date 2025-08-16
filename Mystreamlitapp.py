import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import psycopg2 as pg


# Creating connection with Postgres DB
pgdb = pg.connect(host="dpg-d292oh7diees73fhkbt0-a.singapore-postgres.render.com",
                   user="postgres_online_tykr_user",
                   password="m2SQ6UPiVFM4HxteTBXVrBC8qFtVQv8p",
                   database= "postgres_online_tykr"
                  )
pgcursor = pgdb.cursor()

st.set_page_config(
    page_title="PhonePe Pulse — DB Dashboard",
    layout="wide",
    page_icon="📊",
)
st.title("📊 PhonePe Pulse — Database Powered Dashboard")

def load_india_states_geojson():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    return requests.get(url, timeout=30).json()

india_geo = load_india_states_geojson()

# # ------------------------------
# # Sidebar Filters
# # ------------------------------
st.sidebar.header("Filters")
Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2024)
Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)

# st.sidebar.caption("Tip: Ensure `state` values match India GeoJSON `ST_NM` (e.g., 'Tamil Nadu', 'Karnataka').")

# # ------------------------------
# # Tabs
# # ------------------------------
st.set_page_config(layout="wide")

# Inject CSS to style tabs
st.markdown(
    """
    <style>
    /* Make tabs bigger and fill width */
    div[data-baseweb="tab-list"] {
        display: flex;
        justify-content: space-between;
    }

    div[data-baseweb="tab"] {
        flex-grow: 1 !important;      /* Make tabs expand equally */
        text-align: center !important;
        font-size: 24px !important;   /* Bigger font */
        padding: 15px !important;     /* Bigger height */
        margin: 0 !important;            /* Remove gaps */
        border-radius: 0 !important;     /* Square edges (button look) */
        border: 1px solid #ddd !important;
        background-color: #f0f2f6 !important;
    }

    /* Highlight active tab */
    div[data-baseweb="tab"][aria-selected="true"] {
        background-color: #1f77b4 !important;
        color: white !important;
        font-weight: 700 !important;
    }

    /* Inactive tabs */
    div[data-baseweb="tab"][aria-selected="false"] {
        background-color: #f0f2f6 !important;
        color: black !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

tab_abt,tab_txn, tab_users, tab_exp = st.tabs([ "📃About","💰 Transactions", "👥 Users","💹 Explore Data"])

# # =====================================================
# # ABOUT TAB
# # =====================================================
with tab_abt:
    
    st.markdown("# :violet[Data Visualization and Exploration]")
    st.markdown("## :violet[A User-Friendly Tool Using Streamlit and Plotly]")
    col1,col2 = st.columns([3,2],gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :violet[Domain :] Fintech")
        st.markdown("### :violet[Technologies used :] Github Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")
        st.markdown("### :violet[Overview :] In this streamlit web app you can visualize the phonepe pulse data and gain lot of insights on transactions, number of users, top 10 state, district, pincode and which brand has most number of users and so on. Bar charts, Pie charts and Geo map visualization are used to get some insights.")
    with col2:
        # st.image("home.png")
        st.write(" ")
# # =====================================================
# # TRANSACTIONS TAB
# # =====================================================

def kpi_row(cols, labels, values, delta=None):
    for i, c in enumerate(cols):
        c.metric(labels[i], values[i], (delta[i] if delta else None))
def safe_number(x):
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)
    
with tab_txn:
    st.subheader(f"Transactions — {Year} Q{Quarter}")

    txn_metric = st.radio(
        "Metric",
        ["Amount", "Count"],
        horizontal=True,
        key="txn_metric",
        help="Switch between transaction amount and transaction count."
    )
    if txn_metric == "Amount":
        txn_col = 'Transaction_amount'  
    else:
        txn_col = 'Transaction_Count'

    pgcursor.execute(f"select st.State State,year,quarter,sum(Transaction_count) as Transaction_count , sum(Transaction_amount) as Transaction_amount from aggregated_transaction agt inner join StateName st ON st.State_table = agt.state where year = {Year} and quarter = {Quarter} GROUP BY st.state,quarter,year")
    df_tr = pd.DataFrame(pgcursor.fetchall(), columns=['State','year','quarter', 'Transaction_Count','Transaction_amount'])


    # # Data for the selected quarter

    # pgcursor.execute(f"select sum(Transaction_count) as Total_Transaction_Count, sum(Transaction_amount) as Total from aggregated_transaction where year = {Year} and quarter = {Quarter} ")
    # df = pd.DataFrame(pgcursor.fetchall(), columns=['Transaction_Count','Total_Amount'])

    # KPIs (national)
    c1, c2, c3 = st.columns(3)
    kpi_row(
        [c1, c2, c3],
        ["Total Amount (₹)", "Total Count", "States Covered"],
        [
            safe_number(df_tr.loc[0, "Transaction_amount"]) if not df_tr.empty else "—",
            safe_number(df_tr.loc[0, "Transaction_Count"]) if not df_tr.empty else "—",
            df_tr["State"].nunique() if not df_tr.empty else 0
        ]
    )
# Choropleth Map
    if df_tr.empty:
        st.warning("No transaction data for the selected period.")
    else:
        fig_map = px.choropleth(
            df_tr,
            geojson=india_geo,
            featureidkey="properties.ST_NM",
            locations="State",
            color=txn_col,
            color_continuous_scale="Viridis",
            title=f"{'Transaction Amount (₹)' if txn_col=='Transaction_amount' else 'Transaction_Count'} by State — {Year} Q{Quarter}",
        )
        fig_map.update_geos(fitbounds="locations", visible=False)

        fig_map.update_layout(margin={"r":0,"t":25,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
        

    # # Top-10 bar
        df_tr[txn_col] = pd.to_numeric(df_tr[txn_col], errors='coerce')
        top10 = df_tr.nlargest(10,txn_col)
        fig_bar = px.bar(
            top10,
            x="State", y=txn_col,
            color=txn_col, color_continuous_scale="Viridis",
            title=f"Top 10 States — {txn_metric} ({Year} Q{Quarter})"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

# Trend across quarters in the year
    pgcursor.execute(f"select quarter, SUM({txn_col}) AS total_value from aggregated_transaction where year = {Year} GROUP BY quarter ORDER BY quarter ")
    df_trend = pd.DataFrame(pgcursor.fetchall(), columns=['quarter','total_value'])

    if not df_trend.empty:
        fig_line = px.line(
            df_trend,
            x="quarter", y="total_value",
            markers=True,
            title=f"{'Amount (₹)' if txn_col=='transaction_amount' else 'Transaction_Count'} Trend — {Year}"
        )
        st.plotly_chart(fig_line, use_container_width=True)

# Insights
    st.subheader("💡 Insights")
    if not df_tr.empty:
        top_state = top10.iloc[0]["State"]
        top_val = top10.iloc[0][txn_col]
        low_state = df_tr.nsmallest(1, txn_col).iloc[0]["State"]
        low_val = df_tr.nsmallest(1, txn_col).iloc[0][txn_col]
        st.write(f"- **{top_state}** leads by {txn_metric.lower()} with **{safe_number(top_val)}**.")
        st.write(f"- **{low_state}** is lowest with **{safe_number(low_val)}**.")
        st.write("- Consider regional campaigns where penetration is low and growth potential is high.")
    else:
        st.write("- No data available for insights.")

# =====================================================
# USERS TAB
# =====================================================
with tab_users:
    st.subheader(f"Users — {Year} Q{Quarter}")

    # Metric toggle (typical PhonePe Pulse users dataset fields)
    user_metric = st.radio(
        "Metric",
        ["Registered Users", "App Opens"],
        horizontal=True,
        key="user_metric"
    )
    user_col = "registered_user" if user_metric == "Registered Users" else "app_opens"

    pgcursor.execute(f"SELECT st.state, year, quarter,SUM(registered_user) registered_user,sum(app_opens) app_opens FROM Map_users mp inner join StateName st ON st.State_table = mp.state where year = {Year} and quarter = {Quarter} Group by st.state,quarter,year")
    df_users_q = pd.DataFrame(pgcursor.fetchall(), columns=['state','year','quarter','registered_user','app_opens'])
    # KPIs
    # pgcursor.execute(f"SELECT SUM(registered_user) total_users, sum(app_opens) total_opens FROM Map_users where year = {Year} and quarter = {Quarter}")
    # df_users_nat = pd.DataFrame(pgcursor.fetchall(), columns=['total_users','total_opens'])
   
    c1, c2, c3 = st.columns(3)
    kpi_row(
        [c1, c2, c3],
        ["Total Registered Users", "Total App Opens", "States Covered"],
        [
            safe_number(df_users_q.loc[0, "registered_user"]) if not df_users_q.empty else "—",
            safe_number(df_users_q.loc[0, "app_opens"]) if not df_users_q.empty else "—",
            df_users_q["state"].nunique() if not df_users_q.empty else 0
        ]
    )

    if df_users_q.empty:
        st.warning("No user data for the selected period.")
    else:
        # Map
        fig_map_u = px.choropleth(
            df_users_q,
            geojson=india_geo,
            featureidkey="properties.ST_NM",
            locations="state",
            color=user_col,
            color_continuous_scale="Viridis",
            title=f"{user_metric} by State — {Year} Q{Quarter}",
        )
        fig_map_u.update_geos(fitbounds="locations", visible=False)
        fig_map_u.update_layout(margin={"r":0,"t":25,"l":0,"b":0})
        st.plotly_chart(fig_map_u, use_container_width=True)
    
    # Top-10
        df_users_q[user_col] = pd.to_numeric(df_users_q[user_col], errors='coerce')
        top10_u = df_users_q.nlargest(10, user_col)
        fig_bar_u = px.bar(
            top10_u,
            x="state", y=user_col,
            color=user_col, color_continuous_scale="Plasma",
            title=f"Top 10 States — {user_metric}"
        )
        st.plotly_chart(fig_bar_u, use_container_width=True)

    # Trend in the year

    pgcursor.execute(f"SELECT quarter, SUM({user_col}) AS total_value FROM Map_users where year = {Year} GROUP BY quarter ORDER BY quarter")
    df_users_trend = pd.DataFrame(pgcursor.fetchall(), columns=['quarter','total_value'])
   
    if not df_users_trend.empty:
        fig_line_u = px.line(
            df_users_trend,
            x="quarter", y="total_value",
            markers=True,
            title=f"{user_metric} Trend — {Year}"
        )
        st.plotly_chart(fig_line_u, use_container_width=True)
    
    # Insights
    st.subheader("💡 Insights")
    if not df_users_q.empty:
        top_state_u = top10_u.iloc[0]["state"]
        top_val_u = top10_u.iloc[0][user_col]
        st.write(f"- **{top_state_u}** tops **{user_metric.lower()}** with **{safe_number(top_val_u)}**.")
        st.write("- Compare app opens vs registered users to spot engagement gaps.")
    else:
        st.write("- No data available for insights.")
# =====================================================
# Explore TAB
# =====================================================
with tab_exp:
    Type = st.selectbox("**Type**", ("Select","Transactions", "Users"))
    if Type == "Transactions":
        col1,col2,col3 = st.columns([1,1,1],gap="small")
        
        with col1:
            st.markdown("### :blue[State]")
            pgcursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from aggregated_transaction where year = {Year} and quarter = {Quarter} group by state order by Total desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
            fig = px.pie(df, values='Total_Amount',
                             names='State',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset_r,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        with col2:
            st.markdown("### :blue[District]")
            pgcursor.execute(f"select district , sum(Transaction_Count) as Total_Count, sum(transaction_Amount) as Total from map_transaction  where year = {Year} and quarter = {Quarter} group by district order by Total desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['District', 'Transaction_Count','Total_Amount'])

            fig = px.pie(df, values='Total_Amount',
                             names='District',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset_r,
                             hover_data=['Transaction_Count'],
                             labels={'Transaction_Count':'Transaction_Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        with col3:
            st.markdown("### :blue[Pincode]")
            pgcursor.execute(f"select pincode, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from top_transaction_pincode where year = {Year} and quarter = {Quarter} group by pincode order by Total desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['Pincode', 'Transactions_Count','Total_Amount'])
            fig = px.pie(df, values='Total_Amount',
                             names='Pincode',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset_r,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
    # INSIGHTS- USERS          
    if Type == "Users":
        col1,col2,col3,col4 = st.columns([2,2,2,2],gap="small")
        
        with col1:
            st.markdown("### :violet[user_brand]")
            if Year == 2022 and Quarter in [2,3,4]:
                st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
            else:
                pgcursor.execute(f"Select user_brand, sum(user_count) as Total_Count, avg(user_percentage) as Avg_Percentage from aggregated_users where year = {Year} and quarter = {Quarter} group by user_brand order by Total_Count desc limit 10")
                df = pd.DataFrame(pgcursor.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
                fig = px.bar(df,
                             title='Top 10',
                             x="Total_Users",
                             y="Brand",
                             orientation='h',
                             color='Avg_Percentage',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig,use_container_width=True)   
    
        with col2:
            st.markdown("### :violet[District]")
            pgcursor.execute(f"select district, sum(Registered_User) as Total_Users, sum(app_opens) as Total_Appopens from map_users where year = {Year} and quarter = {Quarter} group by district order by Total_Users desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['District', 'Total_Users','Total_Appopens'])
            df.Total_Users = df.Total_Users.astype(float)
            fig = px.bar(df,
                         title='Top 10',
                         x="Total_Users",
                         y="District",
                         orientation='h',
                         color='Total_Users',
                         color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig,use_container_width=True)
              
        with col3:
            st.markdown("### :violet[Pincode]")
            pgcursor.execute(f"select Pincode, sum(registered_User) as Total_Users from top_users_pincode where year = {Year} and quarter = {Quarter} group by Pincode order by Total_Users desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['Pincode', 'Total_Users'])
            fig = px.pie(df,
                         values='Total_Users',
                         names='Pincode',
                         title='Top 10',
                         color_discrete_sequence=px.colors.sequential.Agsunset,
                         hover_data=['Total_Users'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        with col4:
            st.markdown("### :violet[State]")
            pgcursor.execute(f"select state, sum(Registered_user) as Total_Users, sum(App_opens) as Total_Appopens from map_users where year = {Year} and quarter = {Quarter} group by state order by Total_Users desc limit 10")
            df = pd.DataFrame(pgcursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
            fig = px.pie(df, values='Total_Users',
                             names='State',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Appopens'],
                             labels={'Total_Appopens':'Total_Appopens'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

    # pgcursor.execute(f"Select distinct transaction_type from aggregated_transaction")
    # df_ty = pd.DataFrame(pgcursor.fetchall(), columns=['transaction_type'])

    # option = st.selectbox('Select Payment type',df_ty['transaction_type'])

    # pgcursor.execute(f"select st.state, transaction_type,Sum(Transaction_count) Transaction_count from aggregated_transaction agt inner join StateName st ON st.state_table = agt.state where transaction_type = '{option}'  group by st.state, transaction_type;")
    # df_ct = pd.DataFrame(pgcursor.fetchall(), columns=['state','transaction_type','Transaction_count'])

    # if df_ct.empty:
    #     st.warning("No user data for the selected period.")
    # else:
    #     # Map
    #     fig_map_u = px.choropleth(
    #         df_ct,
    #         geojson=india_geo,
    #         featureidkey="properties.ST_NM",
    #         locations="state",
    #         color="Transaction_count",
    #         color_continuous_scale="Blues",
    #         title=f"Transaction type count by State",
    #     )
    #     fig_map_u.update_geos(fitbounds="locations", visible=False)
    #     st.plotly_chart(fig_map_u, use_container_width=True)