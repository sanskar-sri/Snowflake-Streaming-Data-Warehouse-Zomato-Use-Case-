import streamlit as st
import pandas as pd

from snowflake.snowpark.context import get_active_session

# -----------------------------------------------------------------------------
# 0. SESSION & PAGE CONFIG
# -----------------------------------------------------------------------------
session = get_active_session()

st.set_page_config(
    page_title="Zomato Analytics – Snowflake",
    layout="wide"
)

st.title("Zomato-like Food Delivery Analytics")
st.caption("Interactive KPIs on top of the Snowflake MARTS and secure fact layer.")

# Show current user/role – you will switch roles manually in Snowsight
info_df = session.sql(
    "SELECT CURRENT_USER() AS USER_NAME, CURRENT_ROLE() AS ROLE_NAME"
).to_pandas()
user_name = info_df.loc[0, "USER_NAME"]
role_name = info_df.loc[0, "ROLE_NAME"]
st.write(f"🔐 Snowflake session: **{user_name}** / role **{role_name}**")

# -----------------------------------------------------------------------------
# 1. SIDEBAR – FILTERS (DATE, CITY, CUISINE)
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")

# Date bounds from secure fact (already RLS-aware)
date_bounds_df = session.sql("""
    SELECT
      MIN(FULL_DATE)::DATE AS MIN_DATE,
      MAX(FULL_DATE)::DATE AS MAX_DATE
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
""").to_pandas()

if date_bounds_df.isna().any().any():
    st.warning("No data available in ZOMATO_DWH.MARTS.FCT_ORDER_SECURE for this role.")
    st.stop()

min_date = date_bounds_df.loc[0, "MIN_DATE"]
max_date = date_bounds_df.loc[0, "MAX_DATE"]

with st.sidebar:
    # Date filter
    date_range = st.date_input(
        "Order Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = date_range
        end_date = date_range

    # City options (visible to current role)
    city_df = session.sql("""
        SELECT DISTINCT RESTAURANT_CITY
        FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
        WHERE RESTAURANT_CITY IS NOT NULL
        ORDER BY RESTAURANT_CITY
    """).to_pandas()
    city_options = city_df["RESTAURANT_CITY"].tolist()

    selected_cities = st.multiselect(
        "Restaurant City",
        options=city_options,
        default=city_options
    )

    # Cuisine options (visible to current role)
    cuisine_df = session.sql("""
        SELECT DISTINCT CUISINE_PRIMARY
        FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
        WHERE CUISINE_PRIMARY IS NOT NULL
        ORDER BY CUISINE_PRIMARY
    """).to_pandas()
    cuisine_options = cuisine_df["CUISINE_PRIMARY"].tolist()

    selected_cuisines = st.multiselect(
        "Primary Cuisine",
        options=cuisine_options,
        default=cuisine_options
    )

# -----------------------------------------------------------------------------
# 2. BUILD WHERE CLAUSE (DATE + CITY + CUISINE)
# -----------------------------------------------------------------------------
date_filter_sql = f"FULL_DATE BETWEEN '{start_date}' AND '{end_date}'"

city_filter_sql = ""
if selected_cities and len(selected_cities) != len(city_options):
    city_escaped = [c.replace("'", "''") for c in selected_cities]
    city_list = ", ".join([f"'{c}'" for c in city_escaped])
    city_filter_sql = f" AND RESTAURANT_CITY IN ({city_list})"

cuisine_filter_sql = ""
if selected_cuisines and len(selected_cuisines) != len(cuisine_options):
    cuisine_escaped = [c.replace("'", "''") for c in selected_cuisines]
    cuisine_list = ", ".join([f"'{c}'" for c in cuisine_escaped])
    cuisine_filter_sql = f" AND CUISINE_PRIMARY IN ({cuisine_list})"

where_clause_fact = f"WHERE {date_filter_sql}{city_filter_sql}{cuisine_filter_sql}"

st.info(
    f"Filters applied: **{start_date} → {end_date}**, "
    f"Cities = {('All' if len(selected_cities) == len(city_options) else ', '.join(selected_cities))}, "
    f"Cuisines = {('All' if len(selected_cuisines) == len(cuisine_options) else ', '.join(selected_cuisines))}"
)

# -----------------------------------------------------------------------------
# 3. RUN ALL CORE QUERIES ONCE (RE-USED IN TABS)
# -----------------------------------------------------------------------------

# 3.1 Overview KPIs
overview_sql = f"""
    SELECT
      SUM(TOTAL_AMOUNT)            AS GMV,
      SUM(IS_DELIVERED)            AS DELIVERED_ORDERS,
      SUM(IS_CANCELLED)            AS CANCELLED_ORDERS,
      AVG(RATING)                  AS AVG_RATING,
      AVG(END_TO_END_MIN)          AS AVG_DELIVERY_TIME_MIN,
      AVG(SLA_BREACHED)            AS SLA_BREACH_RATE
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
    {where_clause_fact}
"""
overview_df = session.sql(overview_sql).to_pandas()

if overview_df.empty or overview_df["GMV"].isna().all():
    st.warning("No data found for selected filters for this role.")
    st.stop()

# 3.2 Time-series
gmv_ts_sql = f"""
    SELECT
      FULL_DATE,
      SUM(TOTAL_AMOUNT)     AS GMV,
      SUM(IS_DELIVERED)     AS DELIVERED_ORDERS
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
    {where_clause_fact}
    GROUP BY FULL_DATE
    ORDER BY FULL_DATE
"""
gmv_ts_df = session.sql(gmv_ts_sql).to_pandas()

# 3.3 Restaurant performance
rest_sql = f"""
    SELECT
      RESTAURANT_ID,
      RESTAURANT_NAME,
      RESTAURANT_CITY,
      CUISINE_PRIMARY,
      SUM(TOTAL_AMOUNT)          AS GMV,
      SUM(IS_DELIVERED)          AS DELIVERED_ORDERS,
      SUM(IS_CANCELLED)          AS CANCELLED_ORDERS,
      AVG(RATING)                AS AVG_RATING,
      AVG(END_TO_END_MIN)        AS AVG_DELIVERY_TIME_MIN
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
    {where_clause_fact}
    GROUP BY
      RESTAURANT_ID,
      RESTAURANT_NAME,
      RESTAURANT_CITY,
      CUISINE_PRIMARY
    ORDER BY GMV DESC
"""
rest_df = session.sql(rest_sql).to_pandas()

# 3.4 Cuisine KPIs
cuisine_kpi_sql = f"""
    SELECT
      CUISINE_PRIMARY,
      SUM(TOTAL_AMOUNT)          AS GMV,
      SUM(IS_DELIVERED)          AS DELIVERED_ORDERS,
      AVG(RATING)                AS AVG_RATING
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
    {where_clause_fact}
    GROUP BY CUISINE_PRIMARY
    ORDER BY GMV DESC
"""
cuisine_kpi_df = session.sql(cuisine_kpi_sql).to_pandas()

# 3.5 City search → order funnel
city_funnel_sql = f"""
    SELECT
      CUSTOMER_CITY,
      DT,
      TOTAL_SEARCHES,
      TOTAL_ORDERS,
      SEARCH_TO_ORDER_RATIO
    FROM ZOMATO_DWH.MARTS.V_CITY_SEARCH_TO_ORDER
    WHERE DT BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY DT, CUSTOMER_CITY
"""
city_funnel_df = session.sql(city_funnel_sql).to_pandas()

# 3.6 Agent efficiency
agent_sql = """
    SELECT
      AGENT_ID,
      AGENT_NAME,
      AGENT_CITY,
      ORDERS_DELIVERED,
      AVG_DISTANCE_PER_ORDER,
      AVG_DELIVERY_TIME_MIN,
      SLA_BREACH_RATE,
      AVG_ORDERS_PER_DAY
    FROM ZOMATO_DWH.MARTS.V_AGENT_EFFICIENCY_KPI
"""
agent_df = session.sql(agent_sql).to_pandas()

# 3.7 Order-level detail
detail_sql = f"""
    SELECT
      FULL_DATE,
      ORDER_ID,
      RESTAURANT_NAME,
      RESTAURANT_CITY,
      CUISINE_PRIMARY,
      CUSTOMER_NAME,
      CUSTOMER_CITY,
      TOTAL_AMOUNT,
      ORDER_STATUS,
      PAYMENT_METHOD,
      RATING,
      SENTIMENT_SCORE,
      END_TO_END_MIN
    FROM ZOMATO_DWH.MARTS.FCT_ORDER_SECURE
    {where_clause_fact}
    ORDER BY FULL_DATE, ORDER_ID
"""
detail_df = session.sql(detail_sql).to_pandas()

# -----------------------------------------------------------------------------
# 4. TABS LAYOUT
# -----------------------------------------------------------------------------
tab_overview, tab_rest, tab_cuisine, tab_funnel, tab_agents, tab_detail, tab_views = st.tabs(
    [
        "Overview",
        "Restaurants",
        "Cuisines",
        "City Funnel",
        "Agents",
        "Order Detail",
        "MARTS View Explorer"
    ]
)

# -----------------------------------------------------------------------------
# 4.1 OVERVIEW TAB
# -----------------------------------------------------------------------------
with tab_overview:
    st.subheader("Overview KPIs")

    gmv = float(overview_df.loc[0, "GMV"] or 0)
    delivered = int(overview_df.loc[0, "DELIVERED_ORDERS"] or 0)
    cancelled = int(overview_df.loc[0, "CANCELLED_ORDERS"] or 0)
    avg_rating = float(overview_df.loc[0, "AVG_RATING"] or 0)
    avg_delivery_time = float(overview_df.loc[0, "AVG_DELIVERY_TIME_MIN"] or 0)
    sla_breach_rate = float(overview_df.loc[0, "SLA_BREACH_RATE"] or 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("GMV (₹)", f"{gmv:,.0f}")
    col2.metric("Delivered Orders", f"{delivered:,}")
    cancel_rate = (cancelled / (delivered + cancelled) * 100) if (delivered + cancelled) > 0 else 0
    col3.metric("Cancellation Rate", f"{cancel_rate:,.1f}%")
    col4.metric("Avg Rating", f"{avg_rating:,.2f}")

    col5, col6 = st.columns(2)
    col5.metric("Avg Delivery Time (min)", f"{avg_delivery_time:,.1f}")
    col6.metric("SLA Breach Rate", f"{sla_breach_rate * 100:,.1f}%")

    st.markdown("---")
    st.subheader("GMV & Delivered Orders Over Time")

    if gmv_ts_df.empty:
        st.info("No time-series data for selected filters.")
    else:
        gmv_ts_df_plot = gmv_ts_df.set_index("FULL_DATE")
        c1, c2 = st.columns(2)
        with c1:
            st.line_chart(gmv_ts_df_plot["GMV"], height=250)
        with c2:
            st.line_chart(gmv_ts_df_plot["DELIVERED_ORDERS"], height=250)

# -----------------------------------------------------------------------------
# 4.2 RESTAURANTS TAB
# -----------------------------------------------------------------------------
with tab_rest:
    st.subheader("Restaurant Performance (GMV, Orders, Rating)")

    if rest_df.empty:
        st.info("No restaurant-level data for selected filters.")
    else:
        st.dataframe(rest_df, use_container_width=True)

        top_rest = rest_df.nlargest(15, "GMV").copy()
        top_rest.set_index("RESTAURANT_NAME", inplace=True)

        st.markdown("#### Top 15 Restaurants by GMV")
        st.bar_chart(top_rest[["GMV"]], height=300)

# -----------------------------------------------------------------------------
# 4.3 CUISINES TAB
# -----------------------------------------------------------------------------
with tab_cuisine:
    st.subheader("Cuisine Performance")

    if cuisine_kpi_df.empty:
        st.info("No cuisine-level data for selected filters.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.dataframe(cuisine_kpi_df, use_container_width=True)
        with c2:
            cuisine_plot_df = cuisine_kpi_df.set_index("CUISINE_PRIMARY")
            st.bar_chart(cuisine_plot_df["GMV"], height=300)

# -----------------------------------------------------------------------------
# 4.4 CITY FUNNEL TAB
# -----------------------------------------------------------------------------
with tab_funnel:
    st.subheader("Search → Order Funnel by City (V_CITY_SEARCH_TO_ORDER)")

    if city_funnel_df.empty:
        st.info("No search/order funnel data for selected date range.")
    else:
        st.dataframe(city_funnel_df, use_container_width=True)

# -----------------------------------------------------------------------------
# 4.5 AGENTS TAB
# -----------------------------------------------------------------------------
with tab_agents:
    st.subheader("Delivery Agent Efficiency (V_AGENT_EFFICIENCY_KPI)")

    if agent_df.empty:
        st.info("No delivery agent data.")
    else:
        st.dataframe(agent_df, use_container_width=True)

        top_agents = agent_df.nlargest(10, "ORDERS_DELIVERED").copy()
        top_agents.set_index("AGENT_NAME", inplace=True)

        st.markdown("#### Top 10 Agents by Delivered Orders")
        st.bar_chart(top_agents[["ORDERS_DELIVERED"]], height=300)

# -----------------------------------------------------------------------------
# 4.6 ORDER DETAIL TAB
# -----------------------------------------------------------------------------
with tab_detail:
    st.subheader("Order-level Detail from FCT_ORDER_SECURE (RLS + masking)")

    if detail_df.empty:
        st.info("No order-level detail for selected filters.")
    else:
        st.dataframe(detail_df, use_container_width=True)

# -----------------------------------------------------------------------------
# 4.7 MARTS VIEW EXPLORER TAB (ALL VIEWS)
# -----------------------------------------------------------------------------
with tab_views:
    st.subheader("MARTS View Explorer (All Views in ZOMATO_DWH.MARTS)")

    views_df = session.sql("""
        SELECT TABLE_NAME
        FROM ZOMATO_DWH.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = 'MARTS'
        ORDER BY TABLE_NAME
    """).to_pandas()

    if views_df.empty:
        st.info("No views found in ZOMATO_DWH.MARTS for this role.")
    else:
        view_names = views_df["TABLE_NAME"].tolist()
        selected_view = st.selectbox("Select a MARTS view to explore", view_names)

        # Introspect columns to see if we can re-use filters
        cols_df = session.sql(f"""
            SELECT COLUMN_NAME
            FROM ZOMATO_DWH.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'MARTS'
              AND TABLE_NAME = '{selected_view}'
            ORDER BY ORDINAL_POSITION
        """).to_pandas()

        cols = set(cols_df["COLUMN_NAME"].tolist())
        view_where_parts = []

        if "FULL_DATE" in cols:
            view_where_parts.append(f"FULL_DATE BETWEEN '{start_date}' AND '{end_date}'")
        if "RESTAURANT_CITY" in cols and selected_cities and len(selected_cities) != len(city_options):
            city_escaped = [c.replace("'", "''") for c in selected_cities]
            city_list = ", ".join([f"'{c}'" for c in city_escaped])
            view_where_parts.append(f"RESTAURANT_CITY IN ({city_list})")
        if "CUISINE_PRIMARY" in cols and selected_cuisines and len(selected_cuisines) != len(cuisine_options):
            cuisine_escaped = [c.replace("'", "''") for c in selected_cuisines]
            cuisine_list = ", ".join([f"'{c}'" for c in cuisine_escaped])
            view_where_parts.append(f"CUISINE_PRIMARY IN ({cuisine_list})")

        if view_where_parts:
            view_where_clause = " WHERE " + " AND ".join(view_where_parts)
        else:
            view_where_clause = ""

        view_sql = f'SELECT * FROM ZOMATO_DWH.MARTS."{selected_view}"{view_where_clause}'
        st.code(view_sql, language="sql")

        try:
            view_df = session.sql(view_sql).to_pandas()
            if view_df.empty:
                st.info("No rows found in this view for the current filters/role.")
            else:
                st.dataframe(view_df, use_container_width=True)
        except Exception as e:
            st.error(f"Failed to query view {selected_view}: {e}")

# End of app
