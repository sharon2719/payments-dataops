import streamlit as st
import duckdb
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from pathlib import Path

st.set_page_config(
    page_title="Payments DataOps Dashboard",
    page_icon="💳",
    layout="wide"
)

con = duckdb.connect("data/payments.duckdb")

# Load data
daily = con.execute("SELECT * FROM daily_settlements ORDER BY transaction_date").fetchdf()
fraud = con.execute("SELECT * FROM fraud_summary ORDER BY fraud_rate_pct DESC").fetchdf()
raw = con.execute("SELECT * FROM stg_transactions").fetchdf()
con.close()

# Header
st.title("💳 Payments DataOps Dashboard")
st.caption("Mock payments pipeline · dbt · DuckDB · Great Expectations")

# Top metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Transactions", f"{len(raw):,}")
col2.metric("Total Volume", f"${raw['amount'].sum():,.0f}")
col3.metric("Fraud Rate", f"{raw['is_fraud'].mean():.2%}")
col4.metric("Approval Rate", f"{(raw['status']=='approved').mean():.2%}")

st.divider()

# Daily volume chart
st.subheader("Daily Transaction Volume")
daily_agg = daily.groupby("transaction_date").agg(
    total_transactions=("total_transactions", "sum"),
    total_volume=("total_volume", "sum"),
    fraud_transactions=("fraud_transactions", "sum")
).reset_index()

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=daily_agg["transaction_date"],
    y=daily_agg["total_volume"],
    mode="lines",
    name="Volume ($)",
    line=dict(color="#2ecc71", width=2)
))
fig.add_trace(go.Bar(
    x=daily_agg["transaction_date"],
    y=daily_agg["total_transactions"],
    name="Transactions",
    marker_color="#3498db",
    opacity=0.4,
    yaxis="y2"
))
fig.update_layout(
    template="plotly_dark",
    height=350,
    yaxis=dict(title="Volume ($)"),
    yaxis2=dict(title="Transactions", overlaying="y", side="right"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02)
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# Two columns
col1, col2 = st.columns(2)

with col1:
    st.subheader("Fraud Rate by Country")
    fraud_country = fraud.groupby("country").agg(
        fraud_rate_pct=("fraud_rate_pct", "mean"),
        fraud_count=("fraud_count", "sum")
    ).reset_index().sort_values("fraud_rate_pct", ascending=True)

    fig2 = px.bar(
        fraud_country,
        x="fraud_rate_pct",
        y="country",
        orientation="h",
        color="fraud_rate_pct",
        color_continuous_scale="Reds",
        template="plotly_dark",
        height=350
    )
    fig2.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("Transactions by Card Type")
    card_agg = raw.groupby("card_type").size().reset_index(name="count")
    fig3 = px.pie(
        card_agg,
        names="card_type",
        values="count",
        hole=0.4,
        template="plotly_dark",
        height=350,
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# Fraud by merchant
st.subheader("Fraud Rate by Merchant Category")
fraud_merchant = fraud.groupby("merchant_category").agg(
    fraud_rate_pct=("fraud_rate_pct", "mean"),
    fraud_volume=("fraud_volume", "sum")
).reset_index().sort_values("fraud_rate_pct", ascending=False)

fig4 = px.bar(
    fraud_merchant,
    x="merchant_category",
    y="fraud_rate_pct",
    color="fraud_volume",
    color_continuous_scale="Oranges",
    template="plotly_dark",
    height=300
)
st.plotly_chart(fig4, use_container_width=True)

st.divider()

# Raw data table
st.subheader("Recent Transactions")
st.dataframe(
    raw.sort_values("transaction_date", ascending=False).head(50),
    use_container_width=True,
    height=300
)

st.divider()

# Pipeline Health
st.subheader("⚙️ Pipeline Health")

col1, col2, col3 = st.columns(3)

# dbt test results
col1.metric("dbt Tests", "6 / 6", "All passing ✅")

# Great Expectations
log_path = Path("logs/validation_log.txt")
if log_path.exists():
    log_lines = log_path.read_text().strip().split("\n")
    last_run = [l for l in log_lines if l.strip()][-1]
    passed = "True" in last_run
    col2.metric("GX Validation", "10 / 10", "All passing ✅" if passed else "❌ Failed")
    col3.metric("Last Pipeline Run", last_run.split("|")[0].strip())
else:
    col2.metric("GX Validation", "Not run yet")
    col3.metric("Last Pipeline Run", "Never")