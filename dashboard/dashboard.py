import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import time

st.set_page_config(page_title="SaaS Usage Monitor", layout="wide")
st.title("Real-Time SaaS Usage & Overage Monitor")

# Port 5433, not 5432 — this project's Postgres container was mapped to 5433
# to avoid colliding with another local project's Postgres on the default port.
engine = create_engine("postgresql://postgres:postgres@localhost:5433/postgres")

placeholder = st.empty()

while True:
    df = pd.read_sql("""
        SELECT DISTINCT ON (account_id) account_id, plan_tier, total_usage, plan_limit, is_overage, updated_at
        FROM account_usage
        ORDER BY account_id, updated_at DESC
    """, engine)

    # Show usage as a % of each account's own limit, instead of raw usage vs. a
    # separately-scaled limit line — this is what made the first version of this
    # chart unreadable (tiny usage bars next to a 100k-scale limit line zigzagging
    # across the whole plot). One consistent 0-100%+ scale fixes both problems at once.
    df["usage_pct"] = (df["total_usage"] / df["plan_limit"] * 100).round(1)
    df = df.sort_values("usage_pct", ascending=False)

    with placeholder.container():
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Accounts", len(df))
        col2.metric("Accounts in Overage", int(df["is_overage"].sum()))
        col3.metric("Total Usage (all accounts)", int(df["total_usage"].sum()))

        fig = px.bar(
            df, x="account_id", y="usage_pct", color="is_overage",
            color_discrete_map={True: "red", False: "steelblue"},
            title="Live Usage as % of Plan Limit (per account)",
            labels={"usage_pct": "% of plan limit used", "account_id": "Account"},
        )
        # A single flat reference line at 100% instead of a per-account limit
        # line — this is the one line a recruiter needs to read the whole chart:
        # anything crossing it is over its limit.
        fig.add_hline(y=100, line_dash="dash", line_color="orange", annotation_text="Plan Limit (100%)")
        # A unique key per loop iteration — required because this while-loop pattern
        # calls st.plotly_chart() repeatedly within one script run rather than Streamlit
        # auto-rerunning the whole script, so without a key it can't tell the repeated
        # chart calls apart and throws a StreamlitDuplicateElementId error.
        st.plotly_chart(fig, use_container_width=True, key=f"usage_chart_{time.time()}")

        st.subheader("Accounts Currently in Overage")
        st.dataframe(df[df["is_overage"]][["account_id", "plan_tier", "total_usage", "plan_limit", "usage_pct", "updated_at"]])

    time.sleep(5)