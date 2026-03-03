import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

st.set_page_config(page_title="DWS Client Reporting", layout="wide")
st.title("DWS Client Reporting Dashboard")
st.caption("Built with Streamlit in Snowflake | SnowCamp Hands-On Lab")

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.header("Filters")

regions = session.sql(
    "SELECT DISTINCT region FROM SNOWCAMP_LAB.MARTS.D_CLIENT ORDER BY 1"
).to_pandas()["REGION"].tolist()
selected_region = st.sidebar.multiselect("Region", regions, default=regions)

asset_classes = session.sql(
    "SELECT DISTINCT asset_class FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY ORDER BY 1"
).to_pandas()["ASSET_CLASS"].tolist()
selected_asset_class = st.sidebar.multiselect("Asset Class", asset_classes, default=asset_classes)

region_filter = ", ".join([f"'{r}'" for r in selected_region]) if selected_region else "''"
ac_filter = ", ".join([f"'{a}'" for a in selected_asset_class]) if selected_asset_class else "''"

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
kpi_df = session.sql(f"""
    SELECT
        COUNT(DISTINCT f.client_id)    AS total_clients,
        COUNT(DISTINCT f.portfolio_id) AS total_portfolios,
        ROUND(SUM(f.market_value), 0)  AS total_aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter})
      AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
""").to_pandas()

c1, c2, c3 = st.columns(3)
c1.metric("Clients", f"{kpi_df['TOTAL_CLIENTS'].iloc[0]:,}")
c2.metric("Portfolios", f"{kpi_df['TOTAL_PORTFOLIOS'].iloc[0]:,}")
c3.metric("Total AUM", f"${kpi_df['TOTAL_AUM'].iloc[0]:,.0f}")

# ---------------------------------------------------------------------------
# AUM by region
# ---------------------------------------------------------------------------
st.subheader("AUM by Region")
aum_region = session.sql(f"""
    SELECT c.region, ROUND(SUM(f.market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter})
      AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    GROUP BY c.region ORDER BY aum DESC
""").to_pandas()
st.bar_chart(aum_region.set_index("REGION"))

# ---------------------------------------------------------------------------
# AUM by asset class
# ---------------------------------------------------------------------------
st.subheader("AUM by Asset Class")
aum_ac = session.sql(f"""
    SELECT asset_class, ROUND(SUM(market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter})
      AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    GROUP BY asset_class ORDER BY aum DESC
""").to_pandas()
st.bar_chart(aum_ac.set_index("ASSET_CLASS"))

# ---------------------------------------------------------------------------
# Performance vs benchmark
# ---------------------------------------------------------------------------
st.subheader("Portfolio Performance vs Benchmark (Cumulative)")
perf_df = session.sql(f"""
    SELECT
        as_of_date,
        ROUND(AVG(portfolio_return), 6) AS avg_portfolio_return,
        ROUND(AVG(benchmark_return), 6) AS avg_benchmark_return
    FROM SNOWCAMP_LAB.MARTS.F_PERFORMANCE_DAILY p
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON p.client_id = c.client_id
    WHERE c.region IN ({region_filter})
    GROUP BY as_of_date
    ORDER BY as_of_date
""").to_pandas()

if not perf_df.empty:
    perf_df["CUM_PORTFOLIO"] = (1 + perf_df["AVG_PORTFOLIO_RETURN"]).cumprod() - 1
    perf_df["CUM_BENCHMARK"] = (1 + perf_df["AVG_BENCHMARK_RETURN"]).cumprod() - 1
    chart_data = perf_df[["AS_OF_DATE", "CUM_PORTFOLIO", "CUM_BENCHMARK"]].set_index("AS_OF_DATE")
    st.line_chart(chart_data)

# ---------------------------------------------------------------------------
# Mark-to-Market: Real prices from Snowflake Marketplace
# ---------------------------------------------------------------------------
st.subheader("Mark-to-Market: Real vs Synthetic Prices (Snowflake Marketplace)")
st.caption("Synthetic holdings enriched with real NASDAQ closing prices via Snowflake Marketplace")

mtm_df = session.sql(f"""
    SELECT
        m.ticker,
        m.security_name,
        m.sector,
        SUM(m.quantity)                       AS total_qty,
        ROUND(SUM(m.synthetic_market_value), 0) AS synthetic_value,
        ROUND(SUM(m.mark_to_market_value), 0)   AS market_value,
        ROUND(AVG(m.valuation_diff_pct) * 100, 2) AS avg_diff_pct
    FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA m
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON m.client_id = c.client_id
    WHERE c.region IN ({region_filter})
      AND m.asset_class IN ({ac_filter})
      AND m.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA)
      AND m.market_close_price IS NOT NULL
    GROUP BY m.ticker, m.security_name, m.sector
    ORDER BY market_value DESC
    LIMIT 20
""").to_pandas()

if not mtm_df.empty:
    st.bar_chart(mtm_df.set_index("TICKER")[["SYNTHETIC_VALUE", "MARKET_VALUE"]])
    st.dataframe(mtm_df, width="stretch")
else:
    st.info("Mark-to-market data requires the Snowflake Public Data (Free) Marketplace listing.")

# ---------------------------------------------------------------------------
# Holdings detail
# ---------------------------------------------------------------------------
st.subheader("Holdings Detail")
holdings_df = session.sql(f"""
    SELECT
        f.portfolio_name,
        c.client_name,
        c.region,
        f.security_name,
        f.isin,
        f.sector,
        f.asset_class,
        f.currency,
        f.quantity,
        ROUND(f.market_value, 2) AS market_value,
        ROUND(f.portfolio_weight * 100, 2) AS weight_pct
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter})
      AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    ORDER BY f.market_value DESC
    LIMIT 200
""").to_pandas()
st.dataframe(holdings_df, width="stretch")
