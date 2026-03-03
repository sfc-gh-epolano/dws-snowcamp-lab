#!/usr/bin/env python3
"""Generate the enhanced DWS SnowCamp Hands-On Lab Snowflake Notebook (.ipynb)."""
import json
import os

def split_source(text):
    lines = text.split('\n')
    result = []
    for i, line in enumerate(lines):
        if i < len(lines) - 1:
            result.append(line + '\n')
        else:
            if line:
                result.append(line)
    return result

def md_cell(cell_id, text):
    return {"cell_type": "markdown", "id": cell_id,
            "metadata": {"name": cell_id, "collapsed": False},
            "source": split_source(text)}

def sql_cell(cell_id, text):
    return {"cell_type": "code", "id": cell_id,
            "metadata": {"name": cell_id, "language": "sql", "collapsed": False},
            "source": split_source(text), "outputs": [], "execution_count": None}

def py_cell(cell_id, text):
    return {"cell_type": "code", "id": cell_id,
            "metadata": {"name": cell_id, "language": "python", "collapsed": False},
            "source": split_source(text), "outputs": [], "execution_count": None}

cells = []

# =========================================================================
# INTRODUCTION
# =========================================================================

cells.append(md_cell("md_title", """\
# DWS SnowCamp \u2014 Hands-On Lab

## Building an End-to-End Client Reporting Data Product on Snowflake

**Duration**: 1.5 hours | **Level**: Intermediate | **Account**: Snowflake Trial

---

Welcome to the DWS SnowCamp hands-on lab! In this session you will build a complete \
data product pipeline for **asset management client reporting** \u2014 from raw synthetic \
data through to a published, governed data product with an interactive dashboard.

| Part | Topic | Duration |
|------|-------|----------|
| 1 | Environment Setup, Synthetic Data & Marketplace | 20 min |
| 2 | dbt Transformation (Staging \u2192 Marts + Tests + Deploy) | 30 min |
| 3 | Streamlit Dashboard | 20 min |
| 4 | Internal Marketplace & Horizon Catalog | 5 min |
| 5 | Platform Administration & Monitoring | 15 min |

> **Reference**: [Snowflake Notebooks Documentation](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks)"""))

cells.append(md_cell("md_architecture", """\
### Architecture Overview

```
Source Data (Python)  \u2192  RAW schema  \u2192  ANALYTICS schema  \u2192  MARTS schema  \u2192  Streamlit App
                          (Bronze)        (Silver)              (Gold)           \u2193
                                                                       Marketplace Listing
Cybersyn Marketplace  \u2192  ANALYTICS.STG_MARKET_PRICES  \u2192  MARTS.F_HOLDINGS_WITH_MARKET_DATA
(Real NASDAQ prices)       (Staging view)                    (Joined with synthetic holdings)
```

**Three-Layer Architecture** (mirrors the DWS production design):

| Layer | Schema | Purpose |
|-------|--------|---------|
| Bronze / Raw | `RAW` | Synthetic data mimicking on-prem sources |
| Silver / Curated | `ANALYTICS` | Staging views and intermediate tables |
| Gold / Marts | `MARTS` | Business-ready fact and dimension tables |
| Marketplace | `FINANCIAL__ECONOMIC_ESSENTIALS` | Real NASDAQ prices from Cybersyn (zero-copy) |

> In a production DWS deployment, raw data arrives via \
[OpenFlow](https://docs.snowflake.com/en/user-guide/data-load-openflow) \
from on-prem Oracle and SQL Server databases."""))

# =========================================================================
# PART 1: SETUP + SYNTHETIC DATA + MARKETPLACE
# =========================================================================

cells.append(md_cell("md_part1", """\
---
## Part 1: Environment Setup, Synthetic Data & Marketplace (20 min)

We will create the database and warehouse, install the **Cybersyn Financial & Economic \
Essentials** free Marketplace listing, and generate synthetic asset-management data \
using real NASDAQ tickers so we can join with real market prices later.

> **Reference**: \
[CREATE DATABASE](https://docs.snowflake.com/en/sql-reference/sql/create-database) | \
[CREATE WAREHOUSE](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse) | \
[Snowflake Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace)"""))

cells.append(sql_cell("sql_setup", """\
-- =============================================================
-- Create the lab database with three schemas
-- =============================================================

CREATE DATABASE IF NOT EXISTS SNOWCAMP_LAB;

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.RAW
    COMMENT = 'Raw/Bronze layer - synthetic source data';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.ANALYTICS
    COMMENT = 'Staging and intermediate layer - cleaned and enriched';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.MARTS
    COMMENT = 'Gold/Mart layer - business-ready data products';

CREATE WAREHOUSE IF NOT EXISTS WH_LAB
    WAREHOUSE_SIZE   = 'XSMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    COMMENT          = 'SnowCamp hands-on lab warehouse';

-- Enable cross-region inference for Cortex AI features
-- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_LAB;
USE DATABASE SNOWCAMP_LAB;
USE SCHEMA RAW;

SELECT 'Environment ready!' AS status;"""))

cells.append(md_cell("md_marketplace_install", """\
### Install Cybersyn Financial & Economic Essentials (Free)

Before generating data, let's install a **free Marketplace listing** that provides \
real NASDAQ stock prices. We will join this with our synthetic holdings later.

1. In Snowsight, navigate to **Data Products** \u2192 **Marketplace**
2. Search for **Cybersyn Financial Economic Essentials**
3. Click **Get** and accept the terms (no cost)
4. The database `FINANCIAL__ECONOMIC_ESSENTIALS` will appear in your account

> **Reference**: \
[Snowflake Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace) | \
[Cybersyn Listing](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7)"""))

cells.append(sql_cell("sql_validate_cybersyn", """\
-- Validate that the Cybersyn data is accessible
-- You should see stock price records with tickers, dates, and prices
SELECT primary_ticker, variable_name, date, value
FROM FINANCIAL__ECONOMIC_ESSENTIALS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE primary_ticker = 'AAPL'
  AND variable_name = 'Post-Market Close'
ORDER BY date DESC
LIMIT 5;"""))

cells.append(md_cell("md_datamodel", """\
### Data Model

We will generate six tables with **real NASDAQ tickers** so the join with Marketplace data is meaningful:

| Table | Description | Approx. Rows |
|-------|-------------|-------------|
| `CLIENTS` | Institutional clients (pension funds, insurance, etc.) | 30 |
| `PORTFOLIOS` | Investment portfolios linked to clients | 100 |
| `SECURITIES` | Real NASDAQ equities and ETFs (AAPL, MSFT, etc.) | 50 |
| `HOLDINGS` | Daily position snapshots per portfolio | ~5,000 |
| `TRANSACTIONS` | Trade executions (buys and sells) | 10,000 |
| `BENCHMARKS` | Daily returns for 5 benchmark indices | ~1,200 |

> **Reference**: [Snowpark Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)"""))

cells.append(py_cell("py_generate_data", """\
# ================================================================
# Generate synthetic asset-management data with REAL NASDAQ tickers
# ================================================================
import pandas as pd
import random
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

session = get_active_session()
random.seed(42)

# --- Real NASDAQ tickers for joinable Marketplace data ---
TICKER_DATA = [
    ('AAPL',  'Apple Inc',                   'Technology',              'Equity',  'USD'),
    ('MSFT',  'Microsoft Corp',              'Technology',              'Equity',  'USD'),
    ('AMZN',  'Amazon.com Inc',              'Consumer Discretionary',  'Equity',  'USD'),
    ('NVDA',  'NVIDIA Corp',                 'Technology',              'Equity',  'USD'),
    ('GOOGL', 'Alphabet Inc Class A',        'Communication Services',  'Equity',  'USD'),
    ('META',  'Meta Platforms Inc',          'Communication Services',  'Equity',  'USD'),
    ('TSLA',  'Tesla Inc',                   'Consumer Discretionary',  'Equity',  'USD'),
    ('JPM',   'JPMorgan Chase & Co',         'Financials',              'Equity',  'USD'),
    ('V',     'Visa Inc',                    'Financials',              'Equity',  'USD'),
    ('JNJ',   'Johnson & Johnson',           'Healthcare',              'Equity',  'USD'),
    ('UNH',   'UnitedHealth Group Inc',      'Healthcare',              'Equity',  'USD'),
    ('HD',    'Home Depot Inc',              'Consumer Discretionary',  'Equity',  'USD'),
    ('PG',    'Procter & Gamble Co',         'Consumer Discretionary',  'Equity',  'USD'),
    ('MA',    'Mastercard Inc',              'Financials',              'Equity',  'USD'),
    ('XOM',   'Exxon Mobil Corp',            'Energy',                  'Equity',  'USD'),
    ('CVX',   'Chevron Corp',                'Energy',                  'Equity',  'USD'),
    ('LLY',   'Eli Lilly and Co',            'Healthcare',              'Equity',  'USD'),
    ('ABBV',  'AbbVie Inc',                  'Healthcare',              'Equity',  'USD'),
    ('PFE',   'Pfizer Inc',                  'Healthcare',              'Equity',  'USD'),
    ('MRK',   'Merck & Co Inc',              'Healthcare',              'Equity',  'USD'),
    ('COST',  'Costco Wholesale Corp',       'Consumer Discretionary',  'Equity',  'USD'),
    ('WMT',   'Walmart Inc',                 'Consumer Discretionary',  'Equity',  'USD'),
    ('DIS',   'Walt Disney Co',              'Communication Services',  'Equity',  'USD'),
    ('NFLX',  'Netflix Inc',                 'Communication Services',  'Equity',  'USD'),
    ('ADBE',  'Adobe Inc',                   'Technology',              'Equity',  'USD'),
    ('CRM',   'Salesforce Inc',              'Technology',              'Equity',  'USD'),
    ('AMD',   'Advanced Micro Devices Inc',  'Technology',              'Equity',  'USD'),
    ('INTC',  'Intel Corp',                  'Technology',              'Equity',  'USD'),
    ('NEE',   'NextEra Energy Inc',          'Utilities',               'Equity',  'USD'),
    ('DUK',   'Duke Energy Corp',            'Utilities',               'Equity',  'USD'),
    ('SO',    'Southern Co',                 'Utilities',               'Equity',  'USD'),
    ('BLK',   'BlackRock Inc',               'Financials',              'Equity',  'USD'),
    ('GS',    'Goldman Sachs Group Inc',     'Financials',              'Equity',  'USD'),
    ('MS',    'Morgan Stanley',              'Financials',              'Equity',  'USD'),
    ('AMT',   'American Tower Corp',         'Real Estate',             'Equity',  'USD'),
    ('PLD',   'Prologis Inc',                'Real Estate',             'Equity',  'USD'),
    ('SPY',   'SPDR S&P 500 ETF',           'Financials',              'ETF',     'USD'),
    ('QQQ',   'Invesco QQQ Trust',           'Technology',              'ETF',     'USD'),
    ('IWM',   'iShares Russell 2000 ETF',    'Financials',              'ETF',     'USD'),
    ('EFA',   'iShares MSCI EAFE ETF',       'Financials',              'ETF',     'USD'),
    ('VWO',   'Vanguard FTSE Emerging Mkts', 'Financials',              'ETF',     'USD'),
    ('AGG',   'iShares Core US Aggregate',   'Financials',              'ETF',     'USD'),
    ('TLT',   'iShares 20+ Year Treasury',   'Financials',              'ETF',     'USD'),
    ('LQD',   'iShares Investment Grade',    'Financials',              'ETF',     'USD'),
    ('GLD',   'SPDR Gold Shares',            'Materials',               'ETF',     'USD'),
    ('CAT',   'Caterpillar Inc',             'Industrials',             'Equity',  'USD'),
    ('BA',    'Boeing Co',                   'Industrials',             'Equity',  'USD'),
    ('HON',   'Honeywell International',     'Industrials',             'Equity',  'USD'),
    ('LIN',   'Linde PLC',                   'Materials',               'Equity',  'USD'),
    ('FCX',   'Freeport-McMoRan Inc',        'Materials',               'Equity',  'USD'),
]

NUM_CLIENTS       = 30
NUM_PORTFOLIOS    = 100
NUM_HOLDING_DAYS  = 20
NUM_TRANSACTIONS  = 10000
BENCHMARK_DAYS    = 252

REGIONS         = ['EMEA', 'APAC', 'Americas']
AUM_TIERS       = ['Tier 1 (>1B)', 'Tier 2 (500M-1B)', 'Tier 3 (<500M)']
PORTFOLIO_TYPES = ['Equity', 'Fixed Income', 'Multi-Asset', 'ESG', 'Alternatives']
BENCHMARKS      = {'BM001': 'MSCI World', 'BM002': 'Euro Stoxx 50',
                   'BM003': 'S&P 500', 'BM004': 'Bloomberg Global Agg',
                   'BM005': 'FTSE 100'}
FIRST_NAMES     = ['Anna', 'Hans', 'Maria', 'Klaus', 'Sophie', 'Thomas',
                   'Emma', 'Felix', 'Laura', 'Max', 'Julia', 'Stefan',
                   'Lisa', 'Andreas', 'Sarah']
LAST_NAMES      = ['Mueller', 'Schmidt', 'Fischer', 'Weber', 'Meyer',
                   'Wagner', 'Becker', 'Schulz', 'Hoffmann', 'Koch',
                   'Richter', 'Klein', 'Wolf', 'Braun', 'Lange']
COMPANY_SUFFIX  = ['Capital', 'Invest', 'Advisors', 'Partners',
                   'Asset Mgmt', 'Wealth', 'Fund Services']

# ---- 1. CLIENTS ----
clients = []
for i in range(1, NUM_CLIENTS + 1):
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)} {random.choice(COMPANY_SUFFIX)}"
    clients.append({
        'CLIENT_ID': f'CLT{i:04d}', 'CLIENT_NAME': name,
        'REGION': random.choice(REGIONS), 'AUM_TIER': random.choice(AUM_TIERS),
        'ONBOARDING_DATE': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
    })

# ---- 2. PORTFOLIOS ----
portfolios = []
for i in range(1, NUM_PORTFOLIOS + 1):
    client = random.choice(clients)
    portfolios.append({
        'PORTFOLIO_ID': f'PF{i:04d}', 'CLIENT_ID': client['CLIENT_ID'],
        'PORTFOLIO_NAME': f"{client['CLIENT_NAME'].split()[0]} {random.choice(PORTFOLIO_TYPES)} Fund {i}",
        'PORTFOLIO_TYPE': random.choice(PORTFOLIO_TYPES),
        'BENCHMARK_ID': random.choice(list(BENCHMARKS.keys())),
        'INCEPTION_DATE': (datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime('%Y-%m-%d')
    })

# ---- 3. SECURITIES (real NASDAQ tickers) ----
securities = []
for i, (ticker, name, sector, asset_class, ccy) in enumerate(TICKER_DATA, 1):
    securities.append({
        'SECURITY_ID': f'SEC{i:04d}', 'TICKER': ticker,
        'ISIN': f'US{random.randint(1000000000, 9999999999)}',
        'SECURITY_NAME': name, 'SECTOR': sector,
        'ASSET_CLASS': asset_class, 'CURRENCY': ccy
    })

# ---- 4. HOLDINGS ----
holdings = []
hid = 1
base_date = datetime(2025, 1, 6)
for day_offset in range(NUM_HOLDING_DAYS):
    d = base_date + timedelta(days=day_offset)
    if d.weekday() >= 5:
        continue
    for pf in random.sample(portfolios, min(50, len(portfolios))):
        for sec in random.sample(securities, random.randint(3, 8)):
            qty = random.randint(100, 50000)
            price = round(random.uniform(10, 500), 2)
            holdings.append({
                'HOLDING_ID': hid, 'PORTFOLIO_ID': pf['PORTFOLIO_ID'],
                'SECURITY_ID': sec['SECURITY_ID'],
                'AS_OF_DATE': d.strftime('%Y-%m-%d'),
                'QUANTITY': qty, 'MARKET_VALUE': round(qty * price, 2)
            })
            hid += 1

# ---- 5. TRANSACTIONS ----
transactions = []
for i in range(1, NUM_TRANSACTIONS + 1):
    pf = random.choice(portfolios)
    sec = random.choice(securities)
    side = random.choice(['BUY', 'SELL'])
    qty = random.randint(50, 10000)
    price = round(random.uniform(10, 500), 2)
    td = base_date + timedelta(days=random.randint(0, NUM_HOLDING_DAYS - 1))
    transactions.append({
        'TRANSACTION_ID': f'TXN{i:06d}', 'PORTFOLIO_ID': pf['PORTFOLIO_ID'],
        'SECURITY_ID': sec['SECURITY_ID'], 'TRADE_DATE': td.strftime('%Y-%m-%d'),
        'SIDE': side, 'QUANTITY': qty, 'PRICE': price, 'AMOUNT': round(qty * price, 2)
    })

# ---- 6. BENCHMARKS ----
benchmarks = []
for bm_id, bm_name in BENCHMARKS.items():
    for day_offset in range(BENCHMARK_DAYS):
        d = datetime(2024, 2, 1) + timedelta(days=day_offset)
        if d.weekday() >= 5:
            continue
        benchmarks.append({
            'BENCHMARK_ID': bm_id, 'BENCHMARK_NAME': bm_name,
            'AS_OF_DATE': d.strftime('%Y-%m-%d'),
            'DAILY_RETURN': round(random.gauss(0.0003, 0.012), 6)
        })

# ---- Write all tables to Snowflake ----
tables = {
    'CLIENTS': pd.DataFrame(clients),
    'PORTFOLIOS': pd.DataFrame(portfolios),
    'SECURITIES': pd.DataFrame(securities),
    'HOLDINGS': pd.DataFrame(holdings),
    'TRANSACTIONS': pd.DataFrame(transactions),
    'BENCHMARKS': pd.DataFrame(benchmarks),
}
for name, df in tables.items():
    session.write_pandas(df, name, auto_create_table=True, overwrite=True,
                         database='SNOWCAMP_LAB', schema='RAW')
    print(f"  {name}: {len(df):,} rows")

print("\\nAll synthetic data loaded into SNOWCAMP_LAB.RAW!")"""))

cells.append(sql_cell("sql_validate_raw", """\
-- Validate the raw data
SELECT 'CLIENTS'      AS table_name, COUNT(*) AS row_count FROM SNOWCAMP_LAB.RAW.CLIENTS
UNION ALL SELECT 'PORTFOLIOS',   COUNT(*) FROM SNOWCAMP_LAB.RAW.PORTFOLIOS
UNION ALL SELECT 'SECURITIES',   COUNT(*) FROM SNOWCAMP_LAB.RAW.SECURITIES
UNION ALL SELECT 'HOLDINGS',     COUNT(*) FROM SNOWCAMP_LAB.RAW.HOLDINGS
UNION ALL SELECT 'TRANSACTIONS', COUNT(*) FROM SNOWCAMP_LAB.RAW.TRANSACTIONS
UNION ALL SELECT 'BENCHMARKS',   COUNT(*) FROM SNOWCAMP_LAB.RAW.BENCHMARKS
ORDER BY table_name;"""))

cells.append(sql_cell("sql_explore_raw", """\
-- Explore the securities: real NASDAQ tickers
SELECT security_id, ticker, security_name, sector, asset_class
FROM SNOWCAMP_LAB.RAW.SECURITIES
ORDER BY ticker
LIMIT 10;"""))

# =========================================================================
# PART 2: DBT TRANSFORMATION (ENHANCED)
# =========================================================================

cells.append(md_cell("md_part2", """\
---
## Part 2: dbt Transformation (30 min)

Now we transform the raw data into analytics-ready tables using the \
**three-layer pattern** that dbt Projects on Snowflake automate.

| Layer | Purpose | Materialisation |
|-------|---------|-----------------|
| **Staging** | Clean, rename, type-cast raw sources + Cybersyn | Views |
| **Intermediate** | Join, enrich, calculate | Tables |
| **Marts** | Business-ready fact & dimension tables | Tables |

We will first run the transformations manually with SQL, then deploy the \
same logic as a **dbt Project on Snowflake** with automated tests, DAG \
visualisation, and scheduling.

> **Reference**: \
[dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) | \
[CREATE VIEW](https://docs.snowflake.com/en/sql-reference/sql/create-view)"""))

cells.append(md_cell("md_staging", """\
### 2a. Staging Layer (Views)

Staging models clean and rename columns without joining across sources. \
We also create a staging view over the **Cybersyn Marketplace data** \
to extract NASDAQ closing prices."""))

cells.append(sql_cell("sql_staging", """\
USE SCHEMA SNOWCAMP_LAB.ANALYTICS;

-- Staging: Holdings
CREATE OR REPLACE VIEW STG_HOLDINGS AS
SELECT
    holding_id, portfolio_id, security_id, as_of_date, quantity, market_value,
    CASE WHEN quantity > 0 THEN ROUND(market_value / quantity, 4) ELSE 0 END AS unit_price
FROM SNOWCAMP_LAB.RAW.HOLDINGS
WHERE market_value IS NOT NULL;

-- Staging: Transactions
CREATE OR REPLACE VIEW STG_TRANSACTIONS AS
SELECT
    transaction_id, portfolio_id, security_id, trade_date, side,
    quantity, price, amount,
    CASE side WHEN 'BUY' THEN quantity WHEN 'SELL' THEN -quantity ELSE 0 END AS signed_quantity
FROM SNOWCAMP_LAB.RAW.TRANSACTIONS
WHERE price > 0;

-- Staging: Securities (includes TICKER for Marketplace join)
CREATE OR REPLACE VIEW STG_SECURITIES AS
SELECT security_id, ticker, isin, security_name, sector, asset_class, currency
FROM SNOWCAMP_LAB.RAW.SECURITIES;

-- Staging: Cybersyn Marketplace closing prices
CREATE OR REPLACE VIEW STG_MARKET_PRICES AS
SELECT
    primary_ticker AS ticker,
    date           AS price_date,
    value          AS close_price
FROM FINANCIAL__ECONOMIC_ESSENTIALS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE variable_name = 'Post-Market Close'
  AND value IS NOT NULL;

SELECT 'Staging views created (including Cybersyn Marketplace)!' AS status;"""))

cells.append(md_cell("md_intermediate", """\
### 2b. Intermediate Layer (Tables)

Intermediate models **join across staging** and calculate derived metrics. \
Here we enrich holdings with security details and portfolio weights."""))

cells.append(sql_cell("sql_intermediate", """\
CREATE OR REPLACE TABLE SNOWCAMP_LAB.ANALYTICS.INT_PORTFOLIO_POSITIONS AS
WITH portfolio_totals AS (
    SELECT portfolio_id, as_of_date, SUM(market_value) AS total_portfolio_value
    FROM STG_HOLDINGS GROUP BY portfolio_id, as_of_date
)
SELECT
    h.holding_id, h.portfolio_id, h.security_id, h.as_of_date,
    h.quantity, h.market_value, h.unit_price,
    s.ticker, s.isin, s.security_name, s.sector, s.asset_class, s.currency,
    pt.total_portfolio_value,
    CASE WHEN pt.total_portfolio_value > 0
         THEN ROUND(h.market_value / pt.total_portfolio_value, 6) ELSE 0
    END AS portfolio_weight
FROM STG_HOLDINGS h
JOIN STG_SECURITIES s ON h.security_id = s.security_id
JOIN portfolio_totals pt ON h.portfolio_id = pt.portfolio_id AND h.as_of_date = pt.as_of_date;

SELECT COUNT(*) AS intermediate_rows FROM SNOWCAMP_LAB.ANALYTICS.INT_PORTFOLIO_POSITIONS;"""))

cells.append(md_cell("md_marts", """\
### 2c. Mart Layer (Tables)

Mart tables are **business-ready data products**:

- `F_POSITIONS_DAILY` \u2014 Daily holdings with full context
- `F_PERFORMANCE_DAILY` \u2014 Portfolio returns vs benchmark
- `F_HOLDINGS_WITH_MARKET_DATA` \u2014 Holdings joined with **real NASDAQ prices** from Cybersyn
- `D_PORTFOLIO`, `D_CLIENT` \u2014 Dimension tables

> **Reference**: \
[CREATE TABLE AS SELECT](https://docs.snowflake.com/en/sql-reference/sql/create-table)"""))

cells.append(sql_cell("sql_marts", """\
USE SCHEMA SNOWCAMP_LAB.MARTS;

-- Fact: Daily Positions
CREATE OR REPLACE TABLE F_POSITIONS_DAILY AS
SELECT
    p.holding_id, p.portfolio_id, po.client_id, po.portfolio_name, po.portfolio_type,
    po.benchmark_id, p.security_id, p.ticker, p.isin, p.security_name,
    p.sector, p.asset_class, p.currency, p.as_of_date,
    p.quantity, p.market_value, p.unit_price, p.total_portfolio_value, p.portfolio_weight
FROM SNOWCAMP_LAB.ANALYTICS.INT_PORTFOLIO_POSITIONS p
JOIN SNOWCAMP_LAB.RAW.PORTFOLIOS po ON p.portfolio_id = po.portfolio_id;

-- Fact: Daily Performance
CREATE OR REPLACE TABLE F_PERFORMANCE_DAILY AS
WITH portfolio_values AS (
    SELECT portfolio_id, as_of_date, SUM(market_value) AS total_market_value
    FROM SNOWCAMP_LAB.ANALYTICS.STG_HOLDINGS GROUP BY portfolio_id, as_of_date
),
portfolio_returns AS (
    SELECT portfolio_id, as_of_date, total_market_value,
        LAG(total_market_value) OVER (PARTITION BY portfolio_id ORDER BY as_of_date) AS prev_mv,
        CASE WHEN LAG(total_market_value) OVER (PARTITION BY portfolio_id ORDER BY as_of_date) > 0
             THEN ROUND((total_market_value - LAG(total_market_value) OVER (PARTITION BY portfolio_id ORDER BY as_of_date))
                  / LAG(total_market_value) OVER (PARTITION BY portfolio_id ORDER BY as_of_date), 6)
             ELSE 0 END AS daily_return
    FROM portfolio_values
)
SELECT pr.portfolio_id, pf.client_id, pf.benchmark_id, pr.as_of_date, pr.total_market_value,
    pr.daily_return AS portfolio_return, COALESCE(b.daily_return, 0) AS benchmark_return,
    ROUND(pr.daily_return - COALESCE(b.daily_return, 0), 6) AS active_return
FROM portfolio_returns pr
JOIN SNOWCAMP_LAB.RAW.PORTFOLIOS pf ON pr.portfolio_id = pf.portfolio_id
LEFT JOIN SNOWCAMP_LAB.RAW.BENCHMARKS b ON pf.benchmark_id = b.benchmark_id AND pr.as_of_date = b.as_of_date
WHERE pr.prev_mv IS NOT NULL;

-- Dimension: Portfolio
CREATE OR REPLACE TABLE D_PORTFOLIO AS
SELECT p.portfolio_id, p.portfolio_name, p.portfolio_type, p.benchmark_id, p.inception_date,
       p.client_id, c.client_name, c.region, c.aum_tier
FROM SNOWCAMP_LAB.RAW.PORTFOLIOS p
JOIN SNOWCAMP_LAB.RAW.CLIENTS c ON p.client_id = c.client_id;

-- Dimension: Client
CREATE OR REPLACE TABLE D_CLIENT AS
SELECT client_id, client_name, region, aum_tier, onboarding_date
FROM SNOWCAMP_LAB.RAW.CLIENTS;

SELECT 'Core mart tables created!' AS status;"""))

cells.append(md_cell("md_marketplace_join", """\
### 2d. Marketplace Join: Real NASDAQ Prices + Synthetic Holdings

This is the key **Snowflake Marketplace value proposition**: joining your internal data \
with third-party data via **zero-copy data sharing** \u2014 no ETL, no data movement.

We join our synthetic holdings with **real NASDAQ closing prices** from Cybersyn to \
create a mark-to-market view."""))

cells.append(sql_cell("sql_marketplace_mart", """\
-- Fact: Holdings enriched with real NASDAQ prices from Cybersyn Marketplace
CREATE OR REPLACE TABLE SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA AS
SELECT
    p.holding_id, p.portfolio_id, p.client_id,
    p.portfolio_name, p.portfolio_type,
    p.security_id, p.ticker, p.security_name, p.sector, p.asset_class, p.currency,
    p.as_of_date, p.quantity,
    p.market_value       AS synthetic_market_value,
    mkt.close_price      AS market_close_price,
    CASE WHEN mkt.close_price IS NOT NULL
         THEN ROUND(p.quantity * mkt.close_price, 2)
         ELSE p.market_value END AS mark_to_market_value,
    CASE WHEN mkt.close_price IS NOT NULL AND p.market_value > 0
         THEN ROUND((p.quantity * mkt.close_price - p.market_value) / p.market_value, 4)
         ELSE 0 END AS valuation_diff_pct
FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY p
LEFT JOIN SNOWCAMP_LAB.ANALYTICS.STG_MARKET_PRICES mkt
    ON p.ticker = mkt.ticker AND p.as_of_date = mkt.price_date;

-- See the real vs synthetic prices side by side
SELECT ticker, security_name, as_of_date,
       quantity, synthetic_market_value, market_close_price,
       mark_to_market_value, valuation_diff_pct
FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA
WHERE market_close_price IS NOT NULL
ORDER BY mark_to_market_value DESC
LIMIT 15;"""))

cells.append(sql_cell("sql_query_marts", """\
-- Explore the mart: AUM by region
SELECT c.region,
       COUNT(DISTINCT f.portfolio_id) AS portfolios,
       ROUND(SUM(f.market_value), 0) AS total_aum
FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
GROUP BY c.region
ORDER BY total_aum DESC;"""))

# --- dbt Project on Snowflake (enhanced) ---

cells.append(md_cell("md_dbt_project", """\
### 2e. dbt Projects on Snowflake

The SQL above is exactly what **dbt** automates. With \
[dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt), \
you get:

- **Version-controlled models** in Git
- **Automated tests** (not_null, unique, relationships) that run with every build
- **DAG visualization** showing the full dependency graph
- **Deployment** as a Snowflake object with `CREATE DBT PROJECT`
- **Scheduling** via Snowflake Tasks for production pipelines

This lab\u2019s GitHub repo contains a full dbt project at `dbt/snowcamp_client_reporting/` \
with all the models you just ran manually, **plus** schema tests and a Cybersyn staging model.

> **Reference**: \
[dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) | \
[Getting Started Tutorial](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial)"""))

cells.append(md_cell("md_dbt_profiles", """\
### profiles.yml \u2014 Connecting dbt to Snowflake

Every dbt project needs a `profiles.yml` that tells dbt which database, schema, \
warehouse, and role to use. On Snowflake, the `account` and `user` fields can be \
left as placeholders because the project runs under the current session context.

```yaml
snowcamp:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: 'not needed'
      user: 'not needed'
      role: accountadmin
      database: SNOWCAMP_LAB
      schema: ANALYTICS
      warehouse: WH_LAB
    prod:
      type: snowflake
      account: 'not needed'
      user: 'not needed'
      role: accountadmin
      database: SNOWCAMP_LAB
      schema: MARTS
      warehouse: WH_LAB
```

The **dev** target materialises into `ANALYTICS` (staging/intermediate) and the \
**prod** target into `MARTS` (business-ready). This separation enables promotion workflows.

> **Reference**: \
[Understand dbt project objects](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-understanding-dbt-project-objects)"""))

cells.append(md_cell("md_dbt_tests", """\
### dbt Tests \u2014 Automated Data Quality

The repo includes `schema.yml` files that define tests on every model:

**Staging tests** (`models/staging/schema.yml`):
- `holding_id` is `not_null` and `unique`
- `ticker` is `not_null` (ensures every security has a Marketplace-joinable key)
- `transaction_id` is `not_null` and `unique`

**Mart tests** (`models/marts/schema.yml`):
- `portfolio_id` has a `relationships` test to `d_portfolio` (referential integrity)
- `client_id` has a `relationships` test to `d_client`

When you run `dbt build`, these tests execute automatically after the models \
and fail the pipeline if data quality issues are found.

> **Reference**: \
[dbt Tests Documentation](https://docs.getdbt.com/docs/build/data-tests)"""))

cells.append(sql_cell("sql_dbt_project", """\
-- Step 1: Create an API integration for GitHub (may already exist from setup)
CREATE OR REPLACE API INTEGRATION snowcamp_git_api
    API_PROVIDER       = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED            = TRUE;

-- Step 2: Create a Git Repository pointing to the lab repo
CREATE OR REPLACE GIT REPOSITORY SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO
    API_INTEGRATION = snowcamp_git_api
    ORIGIN          = 'https://github.com/sfc-gh-epolano/dws-snowcamp-lab.git';

-- Step 3: Fetch the latest content
ALTER GIT REPOSITORY SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO FETCH;

-- Step 4: Verify the dbt project files
LS @SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO/branches/main/dbt/snowcamp_client_reporting/;"""))

cells.append(md_cell("md_dbt_deploy", """\
### Deploying and Executing the dbt Project

With the Git repository in place, you can create a **dbt Project object** and \
execute it. The `dbt build` command runs all models and tests in dependency order.

> **Reference**: \
[CREATE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project) | \
[EXECUTE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project) | \
[Deploy dbt project objects](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-deploy)"""))

cells.append(sql_cell("sql_dbt_deploy", """\
-- Deploy the dbt Project object from the Git repository
CREATE OR REPLACE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
    FROM @SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO/branches/main/dbt/snowcamp_client_reporting;

-- Execute dbt build (runs all models + tests)
EXECUTE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
    ARGS = 'build --target dev';"""))

cells.append(md_cell("md_dbt_schedule", """\
### Scheduling with Snowflake Tasks

In production, you schedule dbt execution with a **Snowflake Task**. \
Below is an example that runs the dbt project every hour \u2014 we show the SQL \
for reference but don't execute it during the lab.

```sql
-- Example: Schedule dbt build every hour
CREATE OR REPLACE TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD
    WAREHOUSE = WH_LAB
    SCHEDULE  = 'USING CRON 0 * * * * UTC'
AS
    EXECUTE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
        ARGS = 'build --target dev';

-- Resume the task to start the schedule
ALTER TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD RESUME;
```

### DAG Visualization in Snowsight

To see the full dependency graph of your dbt project:

1. Navigate to **Projects** \u2192 **Workspaces** in Snowsight
2. Open (or create) a workspace connected to the Git repository
3. Select the **DAG** tab below the workspace editor
4. You will see all models, tests, and sources as a visual graph

The DAG shows how data flows from `RAW` sources through `STG_*` staging views, \
`INT_*` intermediate tables, and into `F_*` / `D_*` mart tables \u2014 including the \
Cybersyn Marketplace join.

> **Reference**: \
[Supported dbt commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands) | \
[Introduction to Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)"""))

# =========================================================================
# PART 3: STREAMLIT APP
# =========================================================================

cells.append(md_cell("md_part3", """\
---
## Part 3: Streamlit Dashboard (20 min)

Now let\u2019s turn the mart tables into an **interactive client-reporting dashboard** \
using **Streamlit in Snowflake**. The dashboard includes a section showing \
**real market prices** from the Cybersyn Marketplace join.

> **Reference**: \
[Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) | \
[Streamlit API Reference](https://docs.streamlit.io/develop/api-reference)"""))

cells.append(py_cell("py_streamlit", """\
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("DWS Client Reporting Dashboard")
st.caption("Built with Streamlit in Snowflake | SnowCamp Hands-On Lab")

# ---- Sidebar Filters ----
st.sidebar.header("Filters")
regions = session.sql(
    "SELECT DISTINCT region FROM SNOWCAMP_LAB.MARTS.D_CLIENT ORDER BY 1"
).to_pandas()["REGION"].tolist()
selected_region = st.sidebar.multiselect("Region", regions, default=regions)

asset_classes = session.sql(
    "SELECT DISTINCT asset_class FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY ORDER BY 1"
).to_pandas()["ASSET_CLASS"].tolist()
selected_ac = st.sidebar.multiselect("Asset Class", asset_classes, default=asset_classes)

region_filter = ", ".join([f"'{r}'" for r in selected_region]) if selected_region else "''"
ac_filter = ", ".join([f"'{a}'" for a in selected_ac]) if selected_ac else "''"

# ---- KPI Metrics ----
kpi = session.sql(f'''
    SELECT COUNT(DISTINCT f.client_id) AS clients,
           COUNT(DISTINCT f.portfolio_id) AS portfolios,
           ROUND(SUM(f.market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter}) AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
''').to_pandas()
c1, c2, c3 = st.columns(3)
c1.metric("Clients", f"{kpi['CLIENTS'].iloc[0]:,}")
c2.metric("Portfolios", f"{kpi['PORTFOLIOS'].iloc[0]:,}")
c3.metric("Total AUM", f"${kpi['AUM'].iloc[0]:,.0f}")

# ---- AUM by Region ----
st.subheader("AUM by Region")
aum_region = session.sql(f'''
    SELECT c.region, ROUND(SUM(f.market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter}) AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    GROUP BY c.region ORDER BY aum DESC
''').to_pandas()
st.bar_chart(aum_region.set_index("REGION"))

# ---- Mark-to-Market: Real Prices from Cybersyn ----
st.subheader("Mark-to-Market: Real vs Synthetic (Cybersyn Marketplace)")
mtm = session.sql(f'''
    SELECT ticker, security_name,
           SUM(quantity) AS total_qty,
           ROUND(SUM(synthetic_market_value), 0) AS synthetic_val,
           ROUND(SUM(mark_to_market_value), 0) AS market_val
    FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA m
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON m.client_id = c.client_id
    WHERE c.region IN ({region_filter}) AND m.asset_class IN ({ac_filter})
      AND m.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA)
      AND market_close_price IS NOT NULL
    GROUP BY ticker, security_name ORDER BY market_val DESC LIMIT 15
''').to_pandas()
if not mtm.empty:
    st.bar_chart(mtm.set_index("TICKER")[["SYNTHETIC_VAL", "MARKET_VAL"]])
    st.dataframe(mtm, use_container_width=True)

# ---- Holdings Detail ----
st.subheader("Top Holdings")
holdings = session.sql(f'''
    SELECT f.portfolio_name, c.client_name, f.ticker, f.security_name,
           f.sector, f.asset_class, f.quantity,
           ROUND(f.market_value, 2) AS market_value,
           ROUND(f.portfolio_weight * 100, 2) AS weight_pct
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE c.region IN ({region_filter}) AND f.asset_class IN ({ac_filter})
      AND f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    ORDER BY f.market_value DESC LIMIT 100
''').to_pandas()
st.dataframe(holdings, use_container_width=True)"""))

cells.append(md_cell("md_streamlit_deploy", """\
### Deploying as a Standalone Streamlit App

The lab\u2019s GitHub repo includes a ready-to-deploy app at `streamlit/client_reporting_app.py`.

> **Reference**: \
[CREATE STREAMLIT](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit) | \
[Streamlit in Snowflake Overview](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)"""))

cells.append(sql_cell("sql_create_streamlit", """\
LS @SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO/branches/main/streamlit/;

CREATE OR REPLACE STREAMLIT SNOWCAMP_LAB.ANALYTICS.CLIENT_REPORTING_APP
    ROOT_LOCATION  = '@SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO/branches/main/streamlit'
    MAIN_FILE      = 'client_reporting_app.py'
    QUERY_WAREHOUSE = WH_LAB
    TITLE          = 'DWS Client Reporting'
    COMMENT        = 'SnowCamp lab - interactive client reporting dashboard';

SHOW STREAMLITS IN SCHEMA SNOWCAMP_LAB.ANALYTICS;"""))

# =========================================================================
# PART 4: INTERNAL MARKETPLACE & HORIZON
# =========================================================================

cells.append(md_cell("md_part4", """\
---
## Part 4: Internal Marketplace & Horizon Catalog (5 min)

You already installed Cybersyn from the Marketplace in Part 1 and joined it \
with your data in Part 2. Now let\u2019s **tag and publish** your own data product.

> **Reference**: \
[Snowflake Horizon](https://docs.snowflake.com/en/user-guide/governance-horizon) | \
[Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging) | \
[CREATE TAG](https://docs.snowflake.com/en/sql-reference/sql/create-tag)"""))

cells.append(sql_cell("sql_tags", """\
USE SCHEMA SNOWCAMP_LAB.MARTS;

CREATE OR REPLACE TAG DATA_DOMAIN
    ALLOWED_VALUES 'Client Reporting', 'Risk', 'Compliance', 'ESG'
    COMMENT = 'Business domain that owns the data product';

CREATE OR REPLACE TAG CONFIDENTIALITY
    ALLOWED_VALUES 'Public', 'Internal', 'Confidential', 'Restricted'
    COMMENT = 'Data classification level';

CREATE OR REPLACE TAG DATA_STEWARD
    COMMENT = 'Team or person responsible for data quality';

ALTER TABLE F_POSITIONS_DAILY SET TAG
    DATA_DOMAIN = 'Client Reporting', CONFIDENTIALITY = 'Internal',
    DATA_STEWARD = 'DWS Data Engineering';

ALTER TABLE F_HOLDINGS_WITH_MARKET_DATA SET TAG
    DATA_DOMAIN = 'Client Reporting', CONFIDENTIALITY = 'Internal',
    DATA_STEWARD = 'DWS Data Engineering';

ALTER TABLE D_CLIENT SET TAG
    DATA_DOMAIN = 'Client Reporting', CONFIDENTIALITY = 'Confidential',
    DATA_STEWARD = 'DWS Client Onboarding';

SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('F_POSITIONS_DAILY', 'TABLE'));"""))

cells.append(sql_cell("sql_listing", """\
CREATE OR REPLACE SHARE SNOWCAMP_CLIENT_REPORTING_SHARE
    COMMENT = 'Client Reporting Data Product - SnowCamp Lab';

GRANT USAGE ON DATABASE SNOWCAMP_LAB TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT USAGE ON SCHEMA SNOWCAMP_LAB.MARTS TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY   TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.D_PORTFOLIO         TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.D_CLIENT            TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;

SHOW SHARES LIKE 'SNOWCAMP%';"""))

cells.append(md_cell("md_marketplace_ui", """\
### Publishing to the Internal Marketplace (UI)

1. Navigate to **Data Products** \u2192 **Private Sharing**
2. Click **Share** \u2192 select `SNOWCAMP_CLIENT_REPORTING_SHARE`
3. Add title and description, set visibility to **Internal**
4. Publish

> **Reference**: \
[Creating Listings](https://docs.snowflake.com/en/user-guide/data-marketplace-listings) | \
[Internal Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace-internal)"""))

# =========================================================================
# PART 5: PLATFORM ADMINISTRATION
# =========================================================================

cells.append(md_cell("md_part5", """\
---
## Part 5: Platform Administration & Monitoring (15 min)

> **Reference**: \
[ACCOUNT_USAGE Schema](https://docs.snowflake.com/en/sql-reference/account-usage) | \
[Resource Monitors](https://docs.snowflake.com/en/user-guide/resource-monitors) | \
[Query Insights](https://docs.snowflake.com/en/user-guide/ui-snowsight/activity)"""))

cells.append(sql_cell("sql_credit_usage", """\
SELECT warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
GROUP BY warehouse_name ORDER BY total_credits DESC;"""))

cells.append(sql_cell("sql_query_history", """\
SELECT query_id, query_type, warehouse_name, execution_status,
    ROUND(total_elapsed_time / 1000, 2) AS elapsed_sec,
    ROUND(bytes_scanned / 1e6, 2) AS mb_scanned,
    rows_produced, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC LIMIT 15;"""))

cells.append(sql_cell("sql_resource_monitor", """\
CREATE OR REPLACE RESOURCE MONITOR LAB_MONITOR
    WITH CREDIT_QUOTA = 5 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE WH_LAB SET RESOURCE_MONITOR = LAB_MONITOR;
SHOW RESOURCE MONITORS;"""))

cells.append(md_cell("md_snowsight_tour", """\
### Snowsight UI: Monitoring & Governance

| Feature | Where | What It Shows |
|---------|-------|--------------|
| **Query Insights** | Activity \u2192 Query History | Slow queries, scan efficiency |
| **Performance Explorer** | Admin \u2192 Warehouses | Load patterns, auto-scaling |
| **Cost Management** | Admin \u2192 Cost Management | Credit usage, resource monitors |
| **Trust Center** | Admin \u2192 Security | Security posture |
| **Lineage** | Data \u2192 table \u2192 Lineage tab | Upstream/downstream flow |

> **Reference**: \
[Query Insights](https://docs.snowflake.com/en/user-guide/ui-snowsight/activity) | \
[Cost Management](https://docs.snowflake.com/en/user-guide/cost-management) | \
[Trust Center](https://docs.snowflake.com/en/user-guide/ui-snowsight/trust-center)"""))

cells.append(sql_cell("sql_login_history", """\
SELECT user_name, event_type, is_success, client_ip,
    first_authentication_factor, reported_client_type, event_timestamp
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('hour', -3, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC LIMIT 20;"""))

# =========================================================================
# WRAP-UP
# =========================================================================

cells.append(md_cell("md_wrapup", """\
---
## Congratulations!

You have built an **end-to-end client-reporting data product** on Snowflake:

| Step | What You Built |
|------|---------------|
| **Synthetic Data** | 6 raw tables with real NASDAQ tickers |
| **Marketplace** | Joined with Cybersyn real stock prices (zero-copy) |
| **dbt Transformation** | Staging \u2192 Intermediate \u2192 Marts with automated tests |
| **dbt Deployment** | Deployed as a dbt Project object with scheduling |
| **Streamlit Dashboard** | Interactive app with real market data |
| **Horizon Catalog** | Tagged and published as an Internal Marketplace listing |
| **Platform Admin** | Credit monitoring, query analysis, resource monitors |

### Resources

| Topic | Link |
|-------|------|
| Snowflake Notebooks | [docs.snowflake.com/...notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks) |
| dbt Projects on Snowflake | [docs.snowflake.com/...dbt](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) |
| CREATE DBT PROJECT | [docs.snowflake.com/...create-dbt-project](https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project) |
| Streamlit in Snowflake | [docs.snowflake.com/...streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) |
| Horizon Catalog | [docs.snowflake.com/...horizon](https://docs.snowflake.com/en/user-guide/governance-horizon) |
| Cybersyn Marketplace | [app.snowflake.com/marketplace/...](https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7) |
| ACCOUNT_USAGE | [docs.snowflake.com/...account-usage](https://docs.snowflake.com/en/sql-reference/account-usage) |
| Cortex Code | [docs.snowflake.com/...cortex-code](https://docs.snowflake.com/en/user-guide/ui-snowsight/cortex-code) |

> *Thank you for attending DWS SnowCamp!*"""))

# =========================================================================
# ASSEMBLE NOTEBOOK
# =========================================================================

notebook = {
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name": "Streamlit Notebook", "name": "streamlit"},
        "language_info": {"name": "python", "version": "3.11.0"}
    },
    "cells": cells
}

output_path = os.path.join(os.path.dirname(__file__), "notebooks", "DWS_SnowCamp_Lab.ipynb")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=2, ensure_ascii=False)

print(f"Notebook written to {output_path}")
print(f"Total cells: {len(cells)}")
print(f"  Markdown: {sum(1 for c in cells if c['cell_type'] == 'markdown')}")
print(f"  SQL:      {sum(1 for c in cells if c.get('metadata', {}).get('language') == 'sql')}")
print(f"  Python:   {sum(1 for c in cells if c.get('metadata', {}).get('language') == 'python')}")
