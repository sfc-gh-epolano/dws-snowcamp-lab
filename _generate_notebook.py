#!/usr/bin/env python3
"""Generate the DWS SnowCamp Hands-On Lab notebooks.

Produces TWO notebooks:
  - Day 1 (Workspaces):        Setup, Synthetic Data, Marketplace, dbt Transformation & Orchestration
  - Day 2 (Snowflake Notebook): Streamlit Dashboard, Horizon Catalog, Platform Admin, Cortex Code
"""
import json
import os

IMG = "https://raw.githubusercontent.com/sfc-gh-epolano/dws-snowcamp-lab/main/docs/images"

# =========================================================================
# HELPERS
# =========================================================================

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

def write_notebook(cells, path, kernelspec_name="streamlit", kernelspec_display="Streamlit Notebook"):
    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": kernelspec_display, "name": kernelspec_name},
            "language_info": {"name": "python", "version": "3.11.0"}
        },
        "cells": cells
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    md_count = sum(1 for c in cells if c['cell_type'] == 'markdown')
    sql_count = sum(1 for c in cells if c.get('metadata', {}).get('language') == 'sql')
    py_count = sum(1 for c in cells if c.get('metadata', {}).get('language') == 'python')
    print(f"  {os.path.basename(path)}: {len(cells)} cells (Markdown: {md_count}, SQL: {sql_count}, Python: {py_count})")


# #########################################################################
#
#  DAY 1 — Snowflake Workspaces
#  Setup → Synthetic Data → Marketplace → dbt Transformation → Orchestration
#
# #########################################################################

day1 = []

# -------------------------------------------------------------------------
# INTRODUCTION
# -------------------------------------------------------------------------

day1.append(md_cell("d1_title", """\
# DWS SnowCamp — Day 1: Data Engineering in Snowflake Workspaces

## From Raw Data to Production dbt Pipelines

**Duration**: ~1.5 hours  |  **Level**: Intermediate  |  **Account**: Snowflake Trial

---

Welcome to Day 1 of the DWS SnowCamp hands-on lab! Today you will experience \
what it is like to build a **production-grade data pipeline** for asset-management \
client reporting — entirely inside Snowflake.

We will start by generating realistic synthetic data (the kind of thing an asset \
manager like DWS would receive from trading systems and custodians), enrich it with \
**real NASDAQ market prices** from the Snowflake Marketplace, and then build a \
proper **dbt transformation pipeline** that turns messy source tables into clean, \
tested, business-ready data products.

By the end of today you will have:

| What | Why It Matters |
|------|---------------|
| A three-schema database (RAW → ANALYTICS → MARTS) | Mirrors how DWS separates Bronze / Silver / Gold data |
| Six synthetic source tables with real NASDAQ tickers | Realistic test data that joins with Marketplace prices |
| A full dbt project with staging, intermediate, and mart models | Automated, tested, version-controlled transformations |
| A scheduled Snowflake Task running dbt builds | Production-ready orchestration — no external scheduler needed |

Tomorrow in Day 2 we will pick up where we left off and build an interactive \
Streamlit dashboard, publish data products to the Internal Marketplace, and explore \
platform administration.

| Day 1 | Topic | Duration |
|-------|-------|----------|
| 1 | Environment Setup & Synthetic Data | 20 min |
| 2 | Snowflake Marketplace Integration | 10 min |
| 3 | dbt Transformation (Staging → Intermediate → Marts) | 30 min |
| 4 | dbt Projects: Deploy, Test & Orchestrate | 30 min |

> **Reference**: [Snowflake Workspaces](https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces)"""))

day1.append(md_cell("d1_architecture", """\
### Architecture Overview

Before we write any code, let's understand the architecture we are building. \
This three-layer pattern is the standard for modern data platforms — you will \
see it in production at DWS and most large financial institutions.

```
Source Data (Python)  →  RAW schema  →  ANALYTICS schema  →  MARTS schema
                          (Bronze)        (Silver)              (Gold)
                                                                  ↓
                                                          Streamlit App (Day 2)
                                                                  ↓
                                                        Marketplace Listing (Day 2)

Snowflake Marketplace  →  ANALYTICS.STG_MARKET_PRICES  →  MARTS.F_HOLDINGS_WITH_MARKET_DATA
(Real NASDAQ prices)        (Staging view)                    (Joined with synthetic holdings)
```

**Why three layers?** Each layer serves a distinct purpose:

| Layer | Schema | What Happens Here |
|-------|--------|-------------------|
| **Bronze / Raw** | `RAW` | Data lands here exactly as the source system produces it. No transformations — just a faithful copy. |
| **Silver / Curated** | `ANALYTICS` | Staging views clean column names and types. Intermediate tables join across sources and add calculated fields. |
| **Gold / Marts** | `MARTS` | Business-ready fact and dimension tables. These are what analysts, dashboards, and downstream consumers query. |
| **Marketplace** | `FINANCIAL__ECONOMIC_ESSENTIALS` | Real NASDAQ prices via Snowflake's zero-copy data sharing — no ETL, no storage cost. |

In a production DWS deployment, raw data would arrive via \
[OpenFlow](https://docs.snowflake.com/en/user-guide/data-load-openflow) \
from on-prem Oracle and SQL Server databases. Today we simulate that with Python."""))

# -------------------------------------------------------------------------
# PART 1: SETUP + SYNTHETIC DATA
# -------------------------------------------------------------------------

day1.append(md_cell("d1_part1", """\
---
## Part 1: Environment Setup & Synthetic Data (20 min)

Let's start by creating our working environment. Every Snowflake project needs \
a **database** (logical container for all objects), **schemas** (to organise tables \
into layers), and a **warehouse** (compute engine that runs our queries).

Think of the warehouse like a cluster of servers that Snowflake spins up on demand \
and suspends when idle — you only pay for the seconds of compute you actually use. \
For this lab we use a `MEDIUM` warehouse to keep things responsive.

> **Reference**: \
[CREATE DATABASE](https://docs.snowflake.com/en/sql-reference/sql/create-database) | \
[CREATE WAREHOUSE](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse)"""))

day1.append(sql_cell("d1_sql_setup", """\
-- =============================================================
-- Create our lab database with three schemas (one per layer)
-- =============================================================

CREATE DATABASE IF NOT EXISTS SNOWCAMP_LAB;

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.RAW
    COMMENT = 'Raw/Bronze layer - synthetic source data';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.ANALYTICS
    COMMENT = 'Staging and intermediate layer - cleaned and enriched';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.MARTS
    COMMENT = 'Gold/Mart layer - business-ready data products';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.GITREPO
    COMMENT = 'Git repository integrations';

CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.NOTEBOOKS
    COMMENT = 'Snowflake Notebooks';

CREATE WAREHOUSE IF NOT EXISTS WH_LAB
    WAREHOUSE_SIZE   = 'MEDIUM'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    COMMENT          = 'SnowCamp hands-on lab warehouse';

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- External access integration so the Workspaces compute service can reach the open internet
CREATE OR REPLACE NETWORK RULE snowcamp_egress_rule
    MODE       = EGRESS
    TYPE       = HOST_PORT
    VALUE_LIST = ('0.0.0.0:443', '0.0.0.0:80');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION snowcamp_external_access
    ALLOWED_NETWORK_RULES         = (snowcamp_egress_rule)
    ALLOWED_AUTHENTICATION_SECRETS = ()
    ENABLED                        = TRUE;

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_LAB;
USE DATABASE SNOWCAMP_LAB;
USE SCHEMA RAW;

SELECT 'Environment ready!' AS status;"""))

day1.append(md_cell("d1_md_datamodel", """\
### Synthetic Data Model

Now let's populate the RAW schema with realistic asset-management data. In the \
real world, these tables would be fed by trading systems, portfolio accounting \
platforms, and market data vendors. For our lab we generate them with Python — \
but the important thing is that we use **real NASDAQ tickers** (AAPL, MSFT, NVDA, \
etc.) so that later we can join with actual market prices from the Snowflake \
Marketplace. That join is what makes this lab feel real.

Here is what we will create:

| Table | What It Represents | Approx. Rows |
|-------|-------------------|-------------|
| `CLIENTS` | Institutional investors (pension funds, insurers, etc.) | 30 |
| `PORTFOLIOS` | Investment portfolios, each linked to a client | 100 |
| `SECURITIES` | Equities and ETFs traded on NASDAQ | 50 |
| `HOLDINGS` | Daily position snapshots — "who holds what, and how much" | ~5,000 |
| `TRANSACTIONS` | Trade executions (buys and sells) | 10,000 |
| `BENCHMARKS` | Daily returns for benchmark indices (S&P 500, MSCI World, etc.) | ~1,200 |

For an asset manager like DWS, these six tables are the backbone of client \
reporting — every regulatory filing, performance report, and risk calculation \
starts from data like this.

> **Reference**: [Snowpark Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)"""))

day1.append(py_cell("d1_py_generate_data", """\
# ================================================================
# Generate synthetic asset-management data with REAL NASDAQ tickers
# ================================================================
import pandas as pd
import random
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

session = get_active_session()
random.seed(42)

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

clients = []
for i in range(1, NUM_CLIENTS + 1):
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)} {random.choice(COMPANY_SUFFIX)}"
    clients.append({
        'CLIENT_ID': f'CLT{i:04d}', 'CLIENT_NAME': name,
        'REGION': random.choice(REGIONS), 'AUM_TIER': random.choice(AUM_TIERS),
        'ONBOARDING_DATE': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
    })

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

securities = []
for i, (ticker, name, sector, asset_class, ccy) in enumerate(TICKER_DATA, 1):
    securities.append({
        'SECURITY_ID': f'SEC{i:04d}', 'TICKER': ticker,
        'ISIN': f'US{random.randint(1000000000, 9999999999)}',
        'SECURITY_NAME': name, 'SECTOR': sector,
        'ASSET_CLASS': asset_class, 'CURRENCY': ccy
    })

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

day1.append(sql_cell("d1_sql_validate_raw", """\
-- Validate the raw data — every table should have rows
SELECT 'CLIENTS'      AS table_name, COUNT(*) AS row_count FROM SNOWCAMP_LAB.RAW.CLIENTS
UNION ALL SELECT 'PORTFOLIOS',   COUNT(*) FROM SNOWCAMP_LAB.RAW.PORTFOLIOS
UNION ALL SELECT 'SECURITIES',   COUNT(*) FROM SNOWCAMP_LAB.RAW.SECURITIES
UNION ALL SELECT 'HOLDINGS',     COUNT(*) FROM SNOWCAMP_LAB.RAW.HOLDINGS
UNION ALL SELECT 'TRANSACTIONS', COUNT(*) FROM SNOWCAMP_LAB.RAW.TRANSACTIONS
UNION ALL SELECT 'BENCHMARKS',   COUNT(*) FROM SNOWCAMP_LAB.RAW.BENCHMARKS
ORDER BY table_name;"""))

day1.append(sql_cell("d1_sql_explore_raw", """\
-- Take a peek at the securities — notice these are real NASDAQ tickers
SELECT security_id, ticker, security_name, sector, asset_class
FROM SNOWCAMP_LAB.RAW.SECURITIES
ORDER BY ticker
LIMIT 10;"""))

# -------------------------------------------------------------------------
# PART 2: MARKETPLACE
# -------------------------------------------------------------------------

day1.append(md_cell("d1_part2", f"""\
---
## Part 2: Snowflake Marketplace Integration (10 min)

This is one of the most powerful capabilities in Snowflake: the **Marketplace** \
lets you bring in third-party datasets with **zero ETL and zero storage cost**. \
The data provider shares a read-only snapshot of their tables directly into your \
account — Snowflake calls this "zero-copy data sharing."

For our lab, we will install the **Snowflake Public Data (Free)** listing, which \
includes real NASDAQ stock prices updated daily. Because we used real tickers in \
our synthetic data (AAPL, MSFT, NVDA, etc.), we can join our holdings with actual \
market prices to calculate mark-to-market valuations.

This is exactly the workflow an asset manager would follow: combine proprietary \
portfolio data with market data from an external vendor — except here it happens \
with a single SQL JOIN instead of a complex data pipeline.

### Install Snowflake Public Data (Free)

Follow the step-by-step instructions in the **Hands-On Lab Instructions** \
(Step 3b) to install the **Snowflake Public Data (Free)** listing from the \
Snowflake Marketplace. The instructions include screenshots to guide you through \
the process.

Once installed, the database `FINANCIAL__ECONOMIC_ESSENTIALS` will appear in \
your account with **no storage cost** (zero-copy data sharing). Then return here \
and continue with the next cell.

> **Reference**: \
[Snowflake Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace)"""))

day1.append(sql_cell("d1_sql_validate_marketplace", """\
-- Let's verify the Marketplace data is accessible
-- You should see Apple's closing prices for recent trading days
SELECT *
FROM FINANCIAL__ECONOMIC_ESSENTIALS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE ticker = 'AAPL'
  AND variable = 'post-market_close_adjusted'
ORDER BY date DESC
LIMIT 10;"""))

# -------------------------------------------------------------------------
# PART 3: DBT TRANSFORMATION
# -------------------------------------------------------------------------

day1.append(md_cell("d1_part3", """\
---
## Part 3: dbt Transformation — Staging → Intermediate → Marts (30 min)

Now we get to the heart of data engineering: transforming raw source data into \
clean, reliable, business-ready tables. We will follow the **three-layer dbt pattern** \
that has become the industry standard.

Why does this matter for an asset manager? Because regulators, clients, and risk \
teams all need to trust the numbers. A well-structured transformation pipeline — \
with tests, lineage, and version control — gives you that trust. Every table in \
the mart layer can be traced back to its source, every column has been validated, \
and every change is tracked in Git.

Let's build each layer step by step, starting with staging.

| Layer | What Happens | Materialisation | Why |
|-------|-------------|-----------------|-----|
| **Staging** | Clean, rename, type-cast raw sources | Views | Cheap (no storage), always up-to-date |
| **Intermediate** | Join across staging, calculate derived fields | Tables | Performance — downstream queries are fast |
| **Marts** | Business-ready facts and dimensions | Tables | Optimised for analysts and dashboards |

> **Reference**: \
[dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) | \
[CREATE VIEW](https://docs.snowflake.com/en/sql-reference/sql/create-view)"""))

day1.append(md_cell("d1_md_staging", """\
### 3a. Staging Layer (Views)

Staging models are the **first line of defence** against messy source data. Their \
job is simple: rename columns to a consistent standard, cast types, and filter out \
obvious garbage — but **never** join across tables. Each staging model maps to \
exactly one source table.

We materialise staging models as **views** rather than tables because they don't \
need to store data — they just provide a clean interface over the raw tables. This \
means they are always up-to-date and cost zero storage.

We also create a staging view over the **Snowflake Marketplace** data to extract \
NASDAQ closing prices. Notice how the Marketplace data is accessed exactly like \
any other table — that's the magic of zero-copy sharing."""))

day1.append(sql_cell("d1_sql_staging", """\
USE SCHEMA SNOWCAMP_LAB.ANALYTICS;

-- Staging: Holdings — add a calculated unit price
CREATE OR REPLACE VIEW STG_HOLDINGS AS
SELECT
    holding_id, portfolio_id, security_id, as_of_date, quantity, market_value,
    CASE WHEN quantity > 0 THEN ROUND(market_value / quantity, 4) ELSE 0 END AS unit_price
FROM SNOWCAMP_LAB.RAW.HOLDINGS
WHERE market_value IS NOT NULL;

-- Staging: Transactions — add signed quantity (negative for sells)
CREATE OR REPLACE VIEW STG_TRANSACTIONS AS
SELECT
    transaction_id, portfolio_id, security_id, trade_date, side,
    quantity, price, amount,
    CASE side WHEN 'BUY' THEN quantity WHEN 'SELL' THEN -quantity ELSE 0 END AS signed_quantity
FROM SNOWCAMP_LAB.RAW.TRANSACTIONS
WHERE price > 0;

-- Staging: Securities — includes TICKER for the Marketplace join
CREATE OR REPLACE VIEW STG_SECURITIES AS
SELECT security_id, ticker, isin, security_name, sector, asset_class, currency
FROM SNOWCAMP_LAB.RAW.SECURITIES;

SELECT 'Core staging views created!' AS status;"""))

day1.append(sql_cell("d1_sql_staging_marketplace", """\
-- Staging: Marketplace closing prices (Snowflake Public Data)
-- This view reaches across to the shared Marketplace database —
-- no ETL, no copy, no storage cost.
CREATE OR REPLACE VIEW SNOWCAMP_LAB.ANALYTICS.STG_MARKET_PRICES AS
SELECT
    ticker,
    date           AS price_date,
    value          AS close_price
FROM FINANCIAL__ECONOMIC_ESSENTIALS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE variable = 'post-market_close_adjusted'
  AND value IS NOT NULL;

-- Verify: a sample of real closing prices
SELECT ticker, price_date, close_price
FROM SNOWCAMP_LAB.ANALYTICS.STG_MARKET_PRICES
WHERE ticker IN ('AAPL', 'MSFT', 'NVDA')
ORDER BY ticker, price_date DESC
LIMIT 10;"""))

day1.append(md_cell("d1_md_intermediate", """\
### 3b. Intermediate Layer (Tables)

Now things get interesting. Intermediate models are where we **join across staging** \
and calculate derived metrics. Think of this as the "enrichment" step — we take \
the clean building blocks from staging and combine them into something more useful.

Here we join holdings with securities to get the ticker and sector for each position, \
and we calculate **portfolio weights** (what percentage of a portfolio's total value \
is allocated to each security). This is a critical metric for risk management — \
regulators want to know if a portfolio is too concentrated in any single name.

We materialise intermediate models as **tables** because downstream queries need \
fast access to the joined data."""))

day1.append(sql_cell("d1_sql_intermediate", """\
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

day1.append(md_cell("d1_md_marts", """\
### 3c. Mart Layer (Tables)

The mart layer is where everything comes together. These tables are **business-ready \
data products** — designed for analysts, dashboards, and regulatory reporting. \
Every column has a clear business meaning, and the data has been validated and \
enriched through the layers below.

We will create:

- **`F_POSITIONS_DAILY`** — The core fact table: daily holdings with full security \
and portfolio context. This is what a portfolio manager queries to see "what do \
we hold?"
- **`F_PERFORMANCE_DAILY`** — Daily portfolio returns vs benchmark. Essential for \
performance attribution and client reporting.
- **`D_PORTFOLIO`** and **`D_CLIENT`** — Dimension tables for filtering and grouping.

> **Reference**: \
[CREATE TABLE AS SELECT](https://docs.snowflake.com/en/sql-reference/sql/create-table)"""))

day1.append(sql_cell("d1_sql_marts", """\
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

-- Fact: Daily Performance (portfolio return vs benchmark)
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

day1.append(md_cell("d1_md_marketplace_join", """\
### 3d. Marketplace Join: Real NASDAQ Prices + Synthetic Holdings

This is the step that really shows the **Snowflake Marketplace value proposition**. \
With a single SQL statement, we join our proprietary portfolio data with real-time \
market prices — no data vendor contract negotiations, no FTP downloads, no CSV \
wrangling. The Marketplace data sits in a shared database and is accessed via a \
standard JOIN.

We create `F_HOLDINGS_WITH_MARKET_DATA`, which enriches every holding with:
- The **real closing price** from NASDAQ
- A **mark-to-market value** (quantity × real price)
- The **valuation difference** between our synthetic prices and real market prices

This is exactly the kind of table a fund administrator would use for NAV \
calculations and client statements."""))

day1.append(sql_cell("d1_sql_marketplace_mart", """\
-- Fact: Holdings enriched with real NASDAQ prices from Snowflake Marketplace
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
    ON p.ticker = mkt.ticker AND p.as_of_date = mkt.price_date;"""))

day1.append(sql_cell("d1_sql_marketplace_query", """\
-- See real vs synthetic prices side by side
-- The valuation_diff_pct shows how far our synthetic prices are from reality
SELECT ticker, security_name, as_of_date,
       quantity, synthetic_market_value, market_close_price,
       mark_to_market_value, valuation_diff_pct
FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA
WHERE market_close_price IS NOT NULL
ORDER BY mark_to_market_value DESC
LIMIT 15;"""))

day1.append(sql_cell("d1_sql_query_marts", """\
-- Quick check: AUM by region — this is the kind of query an analyst runs daily
SELECT c.region,
       COUNT(DISTINCT f.portfolio_id) AS portfolios,
       ROUND(SUM(f.market_value), 0) AS total_aum
FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
GROUP BY c.region
ORDER BY total_aum DESC;"""))

# -------------------------------------------------------------------------
# PART 4: DBT PROJECTS — DEPLOY, TEST & ORCHESTRATE
# -------------------------------------------------------------------------

day1.append(md_cell("d1_part4", """\
---
## Part 4: dbt Projects — Deploy, Test & Orchestrate (30 min)

We have just built the transformation pipeline manually with SQL. That works \
for a lab, but in production you need something more: **version control**, \
**automated testing**, **dependency management**, and **scheduling**. That is \
exactly what [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) \
provide.

In this section we will:
1. Understand how the dbt project in this lab's Git repo is structured
2. Deploy it as a Snowflake-native **dbt Project object**
3. Execute `dbt build` to run all models and tests
4. Set up a **Snowflake Task** to orchestrate the pipeline on a schedule

The key insight is that dbt on Snowflake is **not** an external tool that connects \
to Snowflake via a driver — it runs **inside** Snowflake as a first-class object, \
with the same security, governance, and audit trail as any other SQL command.

> **Reference**: \
[dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) | \
[Getting Started Tutorial](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial)"""))

day1.append(md_cell("d1_md_dbt_profiles", """\
### profiles.yml — Connecting dbt to Snowflake

Every dbt project needs a `profiles.yml` that tells dbt which database, schema, \
warehouse, and role to use. When running dbt inside Snowflake (as opposed to from \
your laptop), the `account` and `user` fields can be left as placeholders because \
the project inherits the session context.

Notice the two **targets**: `dev` writes to the `ANALYTICS` schema (for development \
and testing) while `prod` writes to `MARTS` (for business-ready consumption). This \
pattern enables a **promotion workflow** — you develop and test in `ANALYTICS`, \
and when everything passes, you deploy to `MARTS`.

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

> **Reference**: \
[Understand dbt project objects](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-understanding-dbt-project-objects)"""))

day1.append(md_cell("d1_md_dbt_tests", """\
### dbt Tests — Automated Data Quality

One of dbt's superpowers is **built-in testing**. Instead of hoping your data is \
correct, you declare expectations in `schema.yml` files and dbt validates them \
automatically with every build.

This lab's repo includes tests at every layer:

**Staging tests** (`models/staging/schema.yml`):
- `holding_id` is `not_null` and `unique` — no orphaned or duplicated positions
- `ticker` is `not_null` — every security must have a Marketplace-joinable key
- `transaction_id` is `not_null` and `unique`

**Mart tests** (`models/marts/schema.yml`):
- `portfolio_id` has a `relationships` test to `d_portfolio` — referential integrity
- `client_id` has a `relationships` test to `d_client`

When you run `dbt build`, models are created first, then tests execute. If any test \
fails, the build stops and you know exactly which data quality rule was violated. \
For a regulated industry like asset management, this kind of automated validation \
is essential.

> **Reference**: \
[dbt Tests Documentation](https://docs.getdbt.com/docs/build/data-tests)"""))

day1.append(md_cell("d1_md_git_setup", """\
### Connect to the Lab's Git Repository

Before we can deploy the dbt project, Snowflake needs access to the Git repository \
that contains the code. We create an **API integration** (which tells Snowflake it \
is allowed to talk to GitHub) and a **Git Repository object** (which points to our \
specific repo).

Once connected, Snowflake can read any file in the repo — dbt models, Streamlit \
apps, configuration files, and more. Think of it as mounting a Git repo as a \
virtual file system inside Snowflake."""))

day1.append(sql_cell("d1_sql_git_setup", """\
-- Create an API integration for GitHub
CREATE OR REPLACE API INTEGRATION snowcamp_git_api
    API_PROVIDER       = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED            = TRUE;

-- Create a Git Repository pointing to the lab repo
CREATE OR REPLACE GIT REPOSITORY SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO
    API_INTEGRATION = snowcamp_git_api
    ORIGIN          = 'https://github.com/sfc-gh-epolano/dws-snowcamp-lab.git';

-- Fetch the latest content
ALTER GIT REPOSITORY SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO FETCH;

-- Verify the dbt project files are accessible
LS @SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO/branches/main/dbt/snowcamp_client_reporting/;"""))

day1.append(md_cell("d1_md_dbt_deploy", """\
### Deploy and Execute the dbt Project

Now we can create a **dbt Project object** in Snowflake. This is a first-class \
Snowflake object — just like a table or a view — that encapsulates the entire dbt \
project: models, tests, sources, and configuration.

When we execute `dbt build`, Snowflake:
1. Reads the model SQL files from the Git repo
2. Resolves the dependency graph (DAG)
3. Executes models in the correct order
4. Runs all tests after each model completes
5. Reports the results

> **Reference**: \
[CREATE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project) | \
[EXECUTE DBT PROJECT](https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project) | \
[Deploy dbt project objects](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-deploy)"""))

day1.append(sql_cell("d1_sql_dbt_deploy", """\
-- Deploy the dbt Project object from the Git repository
CREATE OR REPLACE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
    FROM @SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO/branches/main/dbt/snowcamp_client_reporting;

-- Execute dbt build — this runs all models + tests in dependency order
EXECUTE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
    ARGS = 'build --target dev';"""))

day1.append(md_cell("d1_md_orchestration", """\
### Orchestration with Snowflake Tasks

In production, transformations need to run on a schedule — daily, hourly, or \
whenever new data arrives. Snowflake **Tasks** provide a built-in scheduler so \
you don't need an external orchestrator like Airflow or Prefect.

A Task is simply a SQL statement that runs on a schedule. By wrapping `EXECUTE \
DBT PROJECT` inside a Task, we get a fully automated, production-grade pipeline \
that:
- Runs every hour (or whatever schedule you choose)
- Uses the `WH_LAB` warehouse for compute
- Can be monitored in the Snowsight UI under **Activity → Task History**
- Sends notifications on failure if configured

Let's create and start a Task now. We will schedule it to run every hour, but \
you could set any CRON schedule (e.g., once a day at 6 AM UTC).

> **Reference**: \
[Introduction to Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro) | \
[CREATE TASK](https://docs.snowflake.com/en/sql-reference/sql/create-task)"""))

day1.append(sql_cell("d1_sql_create_task", """\
-- Create a scheduled Task that runs dbt build every hour
CREATE OR REPLACE TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD
    WAREHOUSE = WH_LAB
    SCHEDULE  = 'USING CRON 0 * * * * UTC'
AS
    EXECUTE DBT PROJECT SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING
        ARGS = 'build --target dev';

-- Resume the Task to activate the schedule
-- (Tasks are created in a SUSPENDED state by default)
ALTER TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD RESUME;

-- Verify the Task is running
SHOW TASKS IN SCHEMA SNOWCAMP_LAB.ANALYTICS;"""))

day1.append(md_cell("d1_md_dag", """\
### DAG Visualisation in Snowsight

To see the full dependency graph of your dbt project:

1. Navigate to **Projects** → **Workspaces** in Snowsight
2. Open (or create) a workspace connected to the Git repository
3. Select the **DAG** tab below the workspace editor
4. You will see all models, tests, and sources as a visual graph

The DAG shows how data flows from `RAW` sources through `STG_*` staging views, \
`INT_*` intermediate tables, and into `F_*` / `D_*` mart tables — including the \
Marketplace data join.

This visualisation is incredibly useful for understanding dependencies, debugging \
issues, and communicating your pipeline architecture to stakeholders.

> **Reference**: \
[Supported dbt commands](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands)"""))

# -------------------------------------------------------------------------
# DAY 1 WRAP-UP
# -------------------------------------------------------------------------

day1.append(md_cell("d1_wrapup", """\
---
## Day 1 Complete!

Excellent work. Let's take stock of what you have built today:

| What You Built | Why It Matters |
|----------------|---------------|
| **3-schema database** (RAW → ANALYTICS → MARTS) | Industry-standard Bronze/Silver/Gold architecture |
| **6 synthetic source tables** with real NASDAQ tickers | Realistic test data that mirrors production feeds |
| **Marketplace integration** with real stock prices | Zero-copy access to third-party data — no ETL |
| **Staging → Intermediate → Mart** transformation pipeline | Clean, tested, traceable data lineage |
| **dbt Project object** with automated tests | Version-controlled, testable transformations |
| **Scheduled Snowflake Task** running dbt builds | Production-ready orchestration |

### What's Coming Tomorrow (Day 2)

In Day 2 we will build on top of everything created today:

1. **Streamlit Dashboard** — Turn the mart tables into an interactive client-reporting app with KPIs, charts, and drill-downs
2. **Horizon Catalog & Internal Marketplace** — Tag data products with business metadata and publish them for internal consumers
3. **Platform Administration** — Monitor credit usage, analyse query performance, and set up resource monitors
4. **Cortex Code** — Explore how Snowflake's AI coding assistant can accelerate your work

> *See you tomorrow! Your data will be waiting for you.* 🎿"""))

# -------------------------------------------------------------------------
# DAY 1 TEARDOWN (suspend task to avoid credit usage overnight)
# -------------------------------------------------------------------------

day1.append(md_cell("d1_md_teardown", """\
---
### Housekeeping: Suspend the Task

To avoid unnecessary credit usage overnight, let's suspend the scheduled Task. \
We will resume it again tomorrow if needed.

> If you are not continuing to Day 2, see the full teardown at the end of the \
Day 2 notebook."""))

day1.append(sql_cell("d1_sql_suspend_task", """\
-- Suspend the Task to avoid credit usage while we are not using it
ALTER TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD SUSPEND;

SHOW TASKS IN SCHEMA SNOWCAMP_LAB.ANALYTICS;"""))


# #########################################################################
#
#  DAY 2 — Snowflake Notebook (Legacy)
#  Streamlit → Horizon → Platform Admin → Cortex Code
#
# #########################################################################

day2 = []

# -------------------------------------------------------------------------
# INTRODUCTION
# -------------------------------------------------------------------------

day2.append(md_cell("d2_title", """\
# DWS SnowCamp — Day 2: Dashboards, Governance & AI

## From Mart Tables to Interactive Data Products

**Duration**: ~1.5 hours  |  **Level**: Intermediate  |  **Account**: Same Snowflake Trial as Day 1

---

Welcome back! Yesterday you built a complete data pipeline — from raw synthetic \
data through to tested, scheduled dbt transformations. Today we will take those \
mart tables and turn them into **things people can actually use**: interactive \
dashboards, governed data products, and monitoring for platform administration.

By the end of today you will have:

| What | Why It Matters |
|------|---------------|
| An interactive Streamlit dashboard | Self-service analytics for portfolio managers and clients |
| Horizon Catalog tags and an Internal Marketplace listing | Data governance and discoverability at scale |
| Resource monitors and query analysis | Cost control and performance optimisation |
| Experience with Cortex Code | AI-assisted development inside Snowflake |

| Day 2 | Topic | Duration |
|-------|-------|----------|
| 1 | Streamlit Dashboard | 25 min |
| 2 | Horizon Catalog & Internal Marketplace | 10 min |
| 3 | Platform Administration & Monitoring | 15 min |
| 4 | Cortex Code: What's Next | 10 min |

> **Important**: This notebook runs in a **Snowflake Notebook** (Projects → Notebooks), \
which includes a built-in Streamlit runtime. This is what lets us render interactive \
widgets directly in notebook cells.

> **Reference**: [Snowflake Notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks)"""))

# -------------------------------------------------------------------------
# PREREQUISITE CHECK
# -------------------------------------------------------------------------

day2.append(md_cell("d2_md_prereq", """\
### Prerequisite Check

Before we begin, let's make sure all the objects from Day 1 are in place. The \
query below checks that the key tables and views exist. If anything is missing, \
re-run the Day 1 notebook first."""))

day2.append(sql_cell("d2_sql_prereq", """\
-- Verify Day 1 objects exist
SELECT 'F_POSITIONS_DAILY'        AS object_name, COUNT(*) AS row_count FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY
UNION ALL SELECT 'F_PERFORMANCE_DAILY',      COUNT(*) FROM SNOWCAMP_LAB.MARTS.F_PERFORMANCE_DAILY
UNION ALL SELECT 'F_HOLDINGS_WITH_MARKET_DATA', COUNT(*) FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA
UNION ALL SELECT 'D_PORTFOLIO',              COUNT(*) FROM SNOWCAMP_LAB.MARTS.D_PORTFOLIO
UNION ALL SELECT 'D_CLIENT',                 COUNT(*) FROM SNOWCAMP_LAB.MARTS.D_CLIENT
ORDER BY object_name;"""))

# -------------------------------------------------------------------------
# PART 1: STREAMLIT DASHBOARD
# -------------------------------------------------------------------------

day2.append(md_cell("d2_part1", """\
---
## Part 1: Streamlit Dashboard (25 min)

Now let's turn those carefully built mart tables into something a portfolio \
manager or client would actually see: an **interactive dashboard**. Streamlit \
in Snowflake lets you build rich web applications entirely in Python, and because \
this is a Snowflake Notebook, you can render Streamlit widgets **directly in cells**.

The dashboard we build will show:
- **KPI metrics** — total clients, portfolios, and AUM at a glance
- **AUM by region** — how assets are distributed geographically
- **Mark-to-market comparison** — synthetic vs real NASDAQ prices from the Marketplace
- **Holdings detail** — a searchable table of individual positions

This is the kind of dashboard a DWS relationship manager might open each morning \
to check on their clients' portfolios.

> **Reference**: \
[Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) | \
[CREATE STREAMLIT](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit)"""))

day2.append(md_cell("d2_md_inline_dashboard", """\
### 1a. Inline Streamlit Dashboard

The cell below builds a complete dashboard using Streamlit widgets. Because we \
are in a Snowflake Notebook with the Streamlit runtime, these widgets render \
as interactive elements — metrics cards, bar charts, and data tables — right here \
in the notebook.

Take a moment to read through the code. Notice how we use `session.sql(...)` to \
query our mart tables, convert the results to Pandas DataFrames, and then pass \
them to Streamlit components. This is the Snowpark + Streamlit pattern you will \
use for all your dashboards."""))

day2.append(py_cell("d2_py_streamlit", """\
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("DWS Client Reporting Dashboard")
st.caption("Built with Streamlit in Snowflake | SnowCamp Hands-On Lab")

# ---- KPI Metrics ----
kpi = session.sql('''
    SELECT COUNT(DISTINCT f.client_id) AS clients,
           COUNT(DISTINCT f.portfolio_id) AS portfolios,
           ROUND(SUM(f.market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
''').to_pandas()
c1, c2, c3 = st.columns(3)
c1.metric("Clients", f"{kpi['CLIENTS'].iloc[0]:,}")
c2.metric("Portfolios", f"{kpi['PORTFOLIOS'].iloc[0]:,}")
c3.metric("Total AUM", f"${kpi['AUM'].iloc[0]:,.0f}")

# ---- AUM by Region ----
st.subheader("AUM by Region")
aum_region = session.sql('''
    SELECT c.region, ROUND(SUM(f.market_value), 0) AS aum
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    GROUP BY c.region ORDER BY aum DESC
''').to_pandas()
st.bar_chart(aum_region.set_index("REGION"))

# ---- Mark-to-Market: Real vs Synthetic ----
st.subheader("Mark-to-Market: Real vs Synthetic (Snowflake Marketplace)")
st.caption("Synthetic holdings enriched with real NASDAQ closing prices via Snowflake Marketplace")
mtm = session.sql('''
    SELECT ticker, security_name,
           ROUND(SUM(synthetic_market_value), 0) AS synthetic_val,
           ROUND(SUM(mark_to_market_value), 0)   AS market_val
    FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA
    WHERE market_close_price IS NOT NULL
      AND as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA)
    GROUP BY ticker, security_name
    ORDER BY market_val DESC LIMIT 15
''').to_pandas()
if not mtm.empty:
    st.bar_chart(mtm.set_index("TICKER")[["SYNTHETIC_VAL", "MARKET_VAL"]])

# ---- Top Holdings ----
st.subheader("Top Holdings")
holdings = session.sql('''
    SELECT f.portfolio_name, c.client_name, f.ticker, f.security_name,
           f.sector, f.asset_class, f.quantity,
           ROUND(f.market_value, 2) AS market_value,
           ROUND(f.portfolio_weight * 100, 2) AS weight_pct
    FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY f
    JOIN SNOWCAMP_LAB.MARTS.D_CLIENT c ON f.client_id = c.client_id
    WHERE f.as_of_date = (SELECT MAX(as_of_date) FROM SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY)
    ORDER BY f.market_value DESC LIMIT 50
''').to_pandas()
st.dataframe(holdings, use_container_width=True)"""))

day2.append(md_cell("d2_md_streamlit_deploy", """\
### 1b. Deploy as a Standalone Streamlit App

The inline dashboard above is great for exploration, but for production use you \
want a **standalone Streamlit app** — a dedicated URL that anyone with the right \
role can access, with sidebar filters and a polished layout.

The lab's GitHub repo includes a ready-to-deploy app at \
`streamlit/client_reporting_app.py` with interactive region and asset-class filters, \
cumulative performance charts, and the mark-to-market comparison.

After deploying, you can find it in **Projects → Streamlit** in Snowsight, or \
click the URL from `SHOW STREAMLITS`.

> **Reference**: \
[CREATE STREAMLIT](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit) | \
[Streamlit in Snowflake Overview](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)"""))

day2.append(sql_cell("d2_sql_create_streamlit", """\
-- Make sure we have the latest repo content
ALTER GIT REPOSITORY SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO FETCH;

-- List the Streamlit app files
LS @SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO/branches/main/streamlit/;

-- Deploy the standalone Streamlit app
CREATE OR REPLACE STREAMLIT SNOWCAMP_LAB.ANALYTICS.CLIENT_REPORTING_APP
    ROOT_LOCATION  = '@SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO/branches/main/streamlit'
    MAIN_FILE      = 'client_reporting_app.py'
    QUERY_WAREHOUSE = WH_LAB
    TITLE          = 'DWS Client Reporting'
    COMMENT        = 'SnowCamp lab - interactive client reporting dashboard';

SHOW STREAMLITS IN SCHEMA SNOWCAMP_LAB.ANALYTICS;"""))

# -------------------------------------------------------------------------
# PART 2: INTERNAL MARKETPLACE & HORIZON
# -------------------------------------------------------------------------

day2.append(md_cell("d2_part2", """\
---
## Part 2: Horizon Catalog & Internal Marketplace (10 min)

Yesterday we consumed data from the Marketplace. Now let's flip the script and \
become **data publishers**. In a large organisation like DWS, different teams \
produce data products that other teams need — risk analytics for compliance, \
client data for relationship managers, performance data for fund reporting.

Snowflake's **Horizon Catalog** provides two key capabilities:
1. **Object tagging** — attach business metadata (domain, confidentiality level, \
data steward) to tables so that consumers can discover and understand data products
2. **Internal Marketplace** — publish your data products as shareable listings \
that other teams can install with a click

The combination of tags + shares means you can implement a full **data mesh** \
architecture: each team owns their data product, tags it with governance metadata, \
and publishes it for consumption.

> **Reference**: \
[Snowflake Horizon](https://docs.snowflake.com/en/user-guide/governance-horizon) | \
[Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging)"""))

day2.append(md_cell("d2_md_tagging", """\
### 2a. Object Tagging

Tags are key-value pairs that you attach to any Snowflake object — databases, \
schemas, tables, even individual columns. They power **data classification**, \
**lineage**, and **policy enforcement**.

We will create three tags:
- **`DATA_DOMAIN`** — Which business area owns this data? (Client Reporting, Risk, etc.)
- **`CONFIDENTIALITY`** — How sensitive is this data? (Public, Internal, Confidential, Restricted)
- **`DATA_STEWARD`** — Who is responsible for data quality?

These tags are visible in the Snowsight UI, searchable via SQL, and can drive \
**tag-based masking policies** — for example, automatically masking columns tagged \
as `Confidential` for users without the right role."""))

day2.append(sql_cell("d2_sql_tags", """\
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

day2.append(md_cell("d2_md_share", """\
### 2b. Create a Share for Internal Publishing

A **Share** is how you package data for other Snowflake accounts (or other teams \
within your organisation). You grant access to specific tables, and consumers \
get a read-only view with zero data movement.

This is the same technology that powers the public Marketplace — except here we \
use it for internal data products within the organisation."""))

day2.append(sql_cell("d2_sql_listing", """\
CREATE OR REPLACE SHARE SNOWCAMP_CLIENT_REPORTING_SHARE
    COMMENT = 'Client Reporting Data Product - SnowCamp Lab';

GRANT USAGE ON DATABASE SNOWCAMP_LAB TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT USAGE ON SCHEMA SNOWCAMP_LAB.MARTS TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.F_POSITIONS_DAILY   TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.F_HOLDINGS_WITH_MARKET_DATA TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.D_PORTFOLIO         TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;
GRANT SELECT ON TABLE SNOWCAMP_LAB.MARTS.D_CLIENT            TO SHARE SNOWCAMP_CLIENT_REPORTING_SHARE;

SHOW SHARES LIKE 'SNOWCAMP%';"""))

day2.append(md_cell("d2_md_marketplace_ui", """\
### Publishing to the Internal Marketplace (UI)

Now let's publish your share as a discoverable **data product** in the Internal \
Marketplace. Open a **new browser tab** so you don't interrupt your notebook session.

#### Step 1: Navigate to Internal Sharing

In the Snowsight sidebar, go to **Horizon Catalog** → **Data sharing** → \
**Internal sharing**.

#### Step 2: Create a Listing

1. Click the **Listings** tab at the top.
2. Click **Create Listing**.
3. For the listing title, enter: \
`DWS Client Reporting Data Product - YOUR_FIRST_NAME YOUR_LAST_NAME`

#### Step 3: Add Your Data Product

1. Click the blue **Add Data Product** button.
2. Click **+ Select**, then find and select `SNOWCAMP_CLIENT_REPORTING_SHARE`.
3. Click **Save**.

#### Step 4: Configure Access Control

1. Click **+ Access Control**.
2. For **"Who can access this data product?"** — choose \
**No accounts or roles are pre-approved** (data will only be available by request).
3. For **"Who else can discover the listing and request access?"** — choose \
**Selected accounts and roles**.
4. From the dropdown, find and select **your own account**.

> **Note**: In a real deployment, this is where you would choose the specific \
accounts or roles that should be able to discover and request access to your \
data product.

#### Step 5: Set Up Request Approval

1. Click **Set up request approval flow**.
2. For **"How should the request approval happen?"** — choose \
**Manage Requests in Snowflake**.
3. For **"Approver email for notifications"** — select **Use Custom Email** and \
enter your own email address.
4. Click **Save** to return to the listing page.

#### Step 6: Add Support Contact and Publish

1. Click **Add Support Contact** and enter your own email address.
2. Click the **Publish** button in the top right corner.
3. Click **Done** in the confirmation prompt.

#### Step 7: Find Your Published Listing

1. In the sidebar, go to **Horizon Catalog** → **Catalog** → **Internal Marketplace**.
2. Sort listings by **Most Recent** (the sort button is next to the Create Listing \
button) and find your listing.

Congratulations — you have built and shared your own data product! Other teams \
in your organisation can now discover and request access to it, just like you \
installed the Snowflake Public Data listing in Day 1.

> **Reference**: \
[Creating Listings](https://docs.snowflake.com/en/user-guide/data-marketplace-listings) | \
[Internal Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace-internal)"""))

# -------------------------------------------------------------------------
# PART 3: PLATFORM ADMINISTRATION
# -------------------------------------------------------------------------

day2.append(md_cell("d2_part3", """\
---
## Part 3: Platform Administration & Monitoring (15 min)

As a platform team, you are not just building data products — you are also \
responsible for keeping the platform healthy, cost-effective, and secure. \
Snowflake provides rich observability through the `SNOWFLAKE.ACCOUNT_USAGE` \
schema, the Snowsight UI, and configurable resource monitors.

In this section we will explore the queries and tools that a DWS platform \
administrator would use daily.

> **Reference**: \
[ACCOUNT_USAGE Schema](https://docs.snowflake.com/en/sql-reference/account-usage) | \
[Resource Monitors](https://docs.snowflake.com/en/user-guide/resource-monitors) | \
[Query Insights](https://docs.snowflake.com/en/user-guide/ui-snowsight/activity)"""))

day2.append(md_cell("d2_md_credit_usage", """\
### 3a. Credit Usage

Credits are Snowflake's unit of compute cost. Every query that runs on a warehouse \
consumes credits. Understanding where credits are being spent is the first step to \
cost optimisation — are there runaway queries? Is a warehouse auto-resuming too \
frequently? Should we downsize a warehouse that is overprovisioned?"""))

day2.append(sql_cell("d2_sql_credit_usage", """\
SELECT warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
GROUP BY warehouse_name ORDER BY total_credits DESC;"""))

day2.append(md_cell("d2_md_query_history", """\
### 3b. Query History

The query history lets you identify slow queries, high-scan queries, and failed \
executions. This is your primary tool for performance troubleshooting. Look for \
queries with high `elapsed_sec` or `mb_scanned` — those are candidates for \
optimisation (better clustering, materialised views, or query rewriting)."""))

day2.append(sql_cell("d2_sql_query_history", """\
SELECT query_id, query_type, warehouse_name, execution_status,
    ROUND(total_elapsed_time / 1000, 2) AS elapsed_sec,
    ROUND(bytes_scanned / 1e6, 2) AS mb_scanned,
    rows_produced, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC LIMIT 15;"""))

day2.append(md_cell("d2_md_resource_monitor", """\
### 3c. Resource Monitors

Resource monitors are your safety net against runaway costs. They track credit \
consumption against a quota and can notify or even **suspend** a warehouse when \
the quota is reached. Every production environment should have these configured.

The monitor below sets a daily quota of 5 credits, notifies at 75% and 90%, and \
suspends the warehouse at 100%. In a real deployment, you would set these based \
on your budget and expected workload."""))

day2.append(sql_cell("d2_sql_resource_monitor", """\
CREATE OR REPLACE RESOURCE MONITOR LAB_MONITOR
    WITH CREDIT_QUOTA = 5 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE WH_LAB SET RESOURCE_MONITOR = LAB_MONITOR;
SHOW RESOURCE MONITORS;"""))

day2.append(md_cell("d2_md_snowsight_tour", """\
### Snowsight UI: Monitoring & Governance

Beyond SQL, Snowsight provides rich visual tools for platform management. Take a \
few minutes to explore these in the UI:

| Feature | Where to Find It | What It Shows |
|---------|-----------------|--------------|
| **Query History** | Monitoring → Query history | Slow queries, scan efficiency, execution plans |
| **Performance Explorer** | Monitoring → Performance explorer | Load patterns, queuing, auto-scaling decisions |
| **Cost Management** | Admin → Cost management | Credit usage trends, resource monitor status |
| **Trust Center** | Governance & security → Trust Center | Security posture, encryption status, network policies |
| **Tags & Policies** | Governance & security → Tags & policies | Tag-based masking policies, row access policies |

> **Reference**: \
[Query Insights](https://docs.snowflake.com/en/user-guide/ui-snowsight/activity) | \
[Cost Management](https://docs.snowflake.com/en/user-guide/cost-management) | \
[Trust Center](https://docs.snowflake.com/en/user-guide/ui-snowsight/trust-center)"""))

day2.append(sql_cell("d2_sql_login_history", """\
-- Review login activity — useful for security auditing
SELECT user_name, event_type, is_success, client_ip,
    first_authentication_factor, reported_client_type, event_timestamp
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('hour', -3, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC LIMIT 20;"""))

# -------------------------------------------------------------------------
# PART 4: CORTEX CODE — WHAT'S NEXT
# -------------------------------------------------------------------------

day2.append(md_cell("d2_part4", """\
---
## Part 4: Cortex Code — AI-Assisted Development in Snowflake (10 min)

Over the past two days you have been writing SQL and Python by hand. But \
Snowflake now includes an **AI coding assistant** called \
[Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight) \
that can help you work faster and explore your data more effectively.

Cortex Code is not a chatbot that gives generic answers — it is an **agentic \
assistant** that understands your Snowflake environment: your roles, schemas, \
tables, and SQL syntax. It can generate queries, modify existing code, explain \
complex SQL, and even help with account administration.

### How to Access Cortex Code

1. Look for the **Cortex Code icon** in the lower-right corner of Snowsight
2. Click it to open the assistant panel
3. Type a question or request in natural language
4. Review the generated SQL — you can execute it directly or copy it to your worksheet

### Access Requirements

To use Cortex Code, your role needs these database roles granted:

```sql
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE ACCOUNTADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER  TO ROLE ACCOUNTADMIN;
```

> **Reference**: \
[Cortex Code in Snowsight](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight) | \
[Cross-region inference](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference)"""))

day2.append(md_cell("d2_md_cortex_prompts", """\
### Ideas to Try with Cortex Code

Now that you have a fully built environment with databases, schemas, marts, \
a Streamlit app, and a dbt project, you have a rich playground for Cortex Code. \
Here are some prompts to try — open the Cortex Code panel and paste these in:

#### SQL Development & Exploration

| Try This Prompt | What It Does |
|----------------|-------------|
| *"Show me the top 10 clients by AUM from F_POSITIONS_DAILY"* | Generates a GROUP BY query with the right column names from your schema |
| *"Write a query for the 7-day moving average of portfolio returns"* | Demonstrates window functions tailored to your data |
| *"Why is this query slow? Optimize it."* | Analyses execution plans and suggests improvements |
| *"Explain the SQL in the F_HOLDINGS_WITH_MARKET_DATA table definition"* | Walks through complex JOIN logic step by step |

#### Data Discovery & Governance

| Try This Prompt | What It Does |
|----------------|-------------|
| *"What databases do I have access to?"* | Inventories your environment based on your role |
| *"Find all tables tagged with CONFIDENTIALITY = 'Confidential'"* | Searches Horizon Catalog metadata |
| *"Show the lineage from RAW.HOLDINGS to MARTS.F_POSITIONS_DAILY"* | Traces upstream/downstream dependencies |
| *"List every table in the MARTS schema and summarise key columns"* | Automated documentation generation |

#### dbt Projects

| Try This Prompt | What It Does |
|----------------|-------------|
| *"Create a staging model for a new RISK_FACTORS source table"* | Scaffolds a dbt model with clean column naming |
| *"Add not_null and unique tests to the holdings model"* | Generates schema.yml test definitions |
| *"Create an incremental model for daily position snapshots"* | Implements dbt incremental logic with merge behaviour |
| *"Generate documentation for the snowcamp_client_reporting dbt project"* | Auto-generates model and column descriptions |

#### Infrastructure & Cost

| Try This Prompt | What It Does |
|----------------|-------------|
| *"Which warehouses are using the most credits this week?"* | Queries ACCOUNT_USAGE for cost analysis |
| *"Show me failed queries in the last hour and suggest fixes"* | Combines query history with diagnostic suggestions |
| *"Create a resource monitor for the WH_LAB warehouse with a 10-credit daily limit"* | Generates the DDL with appropriate triggers |

#### Notebooks & Streamlit

| Try This Prompt | What It Does |
|----------------|-------------|
| *"Build a notebook that does EDA on the F_POSITIONS_DAILY table"* | Creates cells with summary stats, distributions, and charts |
| *"Add a bar chart of AUM by sector to the current notebook"* | Generates a matplotlib or Streamlit chart cell |
| *"Create a Streamlit app for portfolio risk analysis"* | Scaffolds a full Streamlit application |

### Tips for Getting the Best Results

1. **Be specific** — include database, schema, and table names in your prompts. \
Cortex Code works best when it knows exactly which objects you mean.
2. **Iterate** — ask follow-up questions to refine the output. "Now add a filter \
for region" or "Make it an incremental model" work well.
3. **Use quick actions** — highlight SQL text in a Workspace file and use the \
quick actions menu for Fix, Format, Explain, and Edit.
4. **Review before running** — Cortex Code shows a diff view for changes. \
Always review the generated SQL before executing.

> **Reference**: \
[Cortex Code in Snowsight](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight) | \
[Cortex Code agent for dbt Projects](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight#cortex-code-agent-for-dbt-projects-on-snowflake)"""))

# -------------------------------------------------------------------------
# WRAP-UP
# -------------------------------------------------------------------------

day2.append(md_cell("d2_wrapup", """\
---
## Congratulations!

Over two days you have built an **end-to-end client-reporting data product** on Snowflake:

| Day | What You Built | Snowflake Feature |
|-----|---------------|-------------------|
| 1 | Three-schema database (RAW → ANALYTICS → MARTS) | CREATE DATABASE / SCHEMA |
| 1 | Six synthetic source tables with real NASDAQ tickers | Snowpark Python |
| 1 | Marketplace integration with real stock prices | Snowflake Marketplace (zero-copy) |
| 1 | Staging → Intermediate → Mart transformation pipeline | SQL / dbt |
| 1 | dbt Project with automated tests and deployment | CREATE / EXECUTE DBT PROJECT |
| 1 | Scheduled orchestration with Snowflake Tasks | CREATE TASK |
| 2 | Interactive Streamlit dashboard | Streamlit in Snowflake |
| 2 | Horizon Catalog tags and Internal Marketplace listing | Object Tagging / Shares |
| 2 | Credit monitoring, query analysis, resource monitors | ACCOUNT_USAGE / Resource Monitors |
| 2 | AI-assisted development with Cortex Code | Cortex Code |

### Resources

| Topic | Link |
|-------|------|
| Snowflake Workspaces | [docs.snowflake.com/...workspaces](https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces) |
| Snowflake Notebooks | [docs.snowflake.com/...notebooks](https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks) |
| dbt Projects on Snowflake | [docs.snowflake.com/...dbt](https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt) |
| CREATE DBT PROJECT | [docs.snowflake.com/...create-dbt-project](https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project) |
| Streamlit in Snowflake | [docs.snowflake.com/...streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) |
| Horizon Catalog | [docs.snowflake.com/...horizon](https://docs.snowflake.com/en/user-guide/governance-horizon) |
| Snowflake Public Data (Free) | [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTSZAS2KCS) |
| ACCOUNT_USAGE | [docs.snowflake.com/...account-usage](https://docs.snowflake.com/en/sql-reference/account-usage) |
| Cortex Code | [docs.snowflake.com/...cortex-code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight) |

> *Thank you for attending DWS SnowCamp!*"""))

# -------------------------------------------------------------------------
# TEARDOWN
# -------------------------------------------------------------------------

day2.append(md_cell("d2_md_teardown", """\
---
## Teardown (Optional)

Run the cell below **only** when you want to remove all lab objects from your account. \
All statements are commented out to prevent accidental execution. \
Uncomment and run when you are ready to clean up."""))

day2.append(sql_cell("d2_sql_teardown", """\
-- =====================================================
-- UNCOMMENT THE LINES BELOW TO TEAR DOWN THE LAB
-- =====================================================

-- Suspend and drop the scheduled Task
-- ALTER TASK SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD SUSPEND;
-- DROP TASK IF EXISTS SNOWCAMP_LAB.ANALYTICS.DBT_HOURLY_BUILD;

-- Drop the Streamlit app
-- DROP STREAMLIT IF EXISTS SNOWCAMP_LAB.ANALYTICS.CLIENT_REPORTING_APP;

-- Drop the dbt project
-- DROP DBT PROJECT IF EXISTS SNOWCAMP_LAB.ANALYTICS.SNOWCAMP_CLIENT_REPORTING;

-- Drop the share
-- DROP SHARE IF EXISTS SNOWCAMP_CLIENT_REPORTING_SHARE;

-- Drop the resource monitor
-- ALTER WAREHOUSE WH_LAB UNSET RESOURCE_MONITOR;
-- DROP RESOURCE MONITOR IF EXISTS LAB_MONITOR;

-- Drop the Git repository and API integration
-- DROP GIT REPOSITORY IF EXISTS SNOWCAMP_LAB.GITREPO.SNOWCAMP_GIT_REPO;
-- DROP API INTEGRATION IF EXISTS snowcamp_git_api;

-- Drop the lab database (removes ALL schemas, tables, views)
-- DROP DATABASE IF EXISTS SNOWCAMP_LAB;

-- Drop the warehouse
-- DROP WAREHOUSE IF EXISTS WH_LAB;

-- Drop the Marketplace database (if you no longer need it)
-- DROP DATABASE IF EXISTS FINANCIAL__ECONOMIC_ESSENTIALS;

SELECT 'Teardown complete — all lab objects removed.' AS status;"""))


# #########################################################################
# WRITE NOTEBOOKS
# #########################################################################

base_dir = os.path.join(os.path.dirname(__file__), "notebooks")

print("Generating notebooks...")

write_notebook(
    day1, os.path.join(base_dir, "DWS_SnowCamp_Day1.ipynb"),
    kernelspec_name="python", kernelspec_display="Python"
)

write_notebook(
    day2, os.path.join(base_dir, "DWS_SnowCamp_Day2.ipynb"),
    kernelspec_name="streamlit", kernelspec_display="Streamlit Notebook"
)

print("Done!")
