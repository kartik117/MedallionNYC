import duckdb
import pandas as pd
import streamlit as st

from medallion_nyc.config import CONGESTION_PRICING_START, GOLD_DIR

st.set_page_config(page_title="MedallionNYC", page_icon="🚕", layout="wide")
st.title("MedallionNYC")
st.caption(f"NYC Yellow Taxi demand, before and after congestion pricing ({CONGESTION_PRICING_START})")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con


def load_gold(table: str) -> pd.DataFrame:
    path = GOLD_DIR / table
    if not path.exists():
        st.error(f"Gold table `{table}` not found at {path}. Run the pipeline first (see README).")
        st.stop()
    return get_connection().execute(f"SELECT * FROM delta_scan('{path}')").df()


demand_shift = load_gold("demand_shift_by_zone")
cbd_fee = load_gold("monthly_cbd_fee_summary").sort_values("pickup_year_month")

total_before = demand_shift["avg_daily_trips_before"].sum()
total_after = demand_shift["avg_daily_trips_after"].sum()
pct_change_citywide = round(100 * (total_after - total_before) / total_before, 1)

col1, col2, col3 = st.columns(3)
col1.metric("Avg daily trips, before", f"{total_before:,.0f}")
col2.metric("Avg daily trips, after", f"{total_after:,.0f}", delta=f"{pct_change_citywide}%")
col3.metric("CBD fee revenue collected (Jan-Mar 2025)", f"${cbd_fee['total_cbd_fee_revenue'].sum():,.0f}")

st.divider()

st.subheader("CBD congestion fee, month by month")
fee_col, pct_col = st.columns(2)
with fee_col:
    st.caption("Total fee revenue collected via Yellow Taxi trips")
    st.bar_chart(cbd_fee.set_index("pickup_year_month")[["total_cbd_fee_revenue"]])
with pct_col:
    st.caption("Share of trips that crossed into the toll zone")
    st.line_chart(cbd_fee.set_index("pickup_year_month")[["pct_trips_charged_cbd_fee"]])

st.divider()

st.subheader("Demand shift by zone")
boroughs = sorted(demand_shift["borough"].dropna().unique())
selected_boroughs = st.multiselect("Borough", boroughs, default=boroughs)
min_baseline = st.slider("Minimum avg daily trips before (filters out near-zero-volume zones)", 0, 50, 5)

filtered = demand_shift[
    demand_shift["borough"].isin(selected_boroughs) & (demand_shift["avg_daily_trips_before"] >= min_baseline)
].sort_values("pct_change")

display_cols = ["borough", "zone_name", "avg_daily_trips_before", "avg_daily_trips_after", "pct_change"]
left, right = st.columns(2)
with left:
    st.markdown("**Biggest demand losses**")
    st.dataframe(filtered.head(10)[display_cols], hide_index=True)
with right:
    st.markdown("**Biggest demand gains**")
    st.dataframe(filtered.tail(10).sort_values("pct_change", ascending=False)[display_cols], hide_index=True)

st.subheader("Average % change in daily demand, by borough")
borough_avg = filtered.groupby("borough")["pct_change"].mean().sort_values()
st.bar_chart(borough_avg)
