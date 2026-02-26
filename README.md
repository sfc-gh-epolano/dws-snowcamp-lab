# DWS SnowCamp -- Hands-On Lab

**Building an End-to-End Client Reporting Data Product on Snowflake**

A 1.5-hour hands-on lab for the DWS SnowCamp training event. Attendees build a complete data-product pipeline in their own Snowflake trial account: synthetic data generation, dbt transformation, Streamlit dashboard, Internal Marketplace listing, and platform administration.

## Prerequisites

- A **Snowflake Trial account** (provided via HOL infrastructure on the day)
- A modern web browser (Chrome, Edge, or Firefox recommended)
- No local software installation required -- everything runs inside Snowsight

## Quick Start

### 1. Log in to your Snowflake trial account

Use the credentials provided at registration.

### 2. Create a Git Repository integration

Open a **SQL Worksheet** in Snowsight and run:

```sql
-- Create the lab database (the Git Repository object needs a database to live in)
CREATE DATABASE IF NOT EXISTS SNOWCAMP_LAB;
CREATE SCHEMA IF NOT EXISTS SNOWCAMP_LAB.RAW;

-- Allow Snowflake to connect to GitHub
CREATE OR REPLACE API INTEGRATION snowcamp_git_api
    API_PROVIDER         = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED              = TRUE;

-- Point to this repository
CREATE OR REPLACE GIT REPOSITORY SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO
    API_INTEGRATION = snowcamp_git_api
    ORIGIN          = 'https://github.com/sfc-gh-epolano/dws-snowcamp-lab.git';

ALTER GIT REPOSITORY SNOWCAMP_LAB.RAW.SNOWCAMP_GIT_REPO FETCH;
```

### 3. Import the notebook

1. Navigate to **Projects** > **Notebooks** in the left sidebar
2. Click **Create from Repository**
3. Select `SNOWCAMP_GIT_REPO` and choose `notebooks/DWS_SnowCamp_Lab.ipynb`
4. Select the `ACCOUNTADMIN` role and `COMPUTE_WH` (or any available warehouse)
5. Click **Create**

### 4. Run the lab

Execute cells sequentially from top to bottom. Each section includes instructions, SQL/Python code, and links to Snowflake documentation.

## Lab Agenda (1.5 hours)

| Part | Topic | Duration | What You Build |
|------|-------|----------|----------------|
| 1 | Environment Setup & Synthetic Data | 20 min | Database, schemas, warehouse, 6 raw tables |
| 2 | dbt Transformation | 25 min | Staging views, intermediate tables, mart fact/dimension tables |
| 3 | Streamlit Dashboard | 20 min | Interactive client-reporting app with charts and filters |
| 4 | Internal Marketplace | 10 min | Horizon tags, share, and data product listing |
| 5 | Platform Administration | 15 min | Credit monitoring, query analysis, resource monitors |

## Repository Structure

```
dws-snowcamp-lab/
├── README.md                              # This file
├── notebooks/
│   └── DWS_SnowCamp_Lab.ipynb            # Main hands-on lab notebook
├── dbt/
│   └── snowcamp_client_reporting/         # dbt Project (deployed in Part 2)
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/                   # Views: clean and rename raw data
│           ├── intermediate/              # Tables: join and enrich
│           └── marts/                     # Tables: business-ready facts and dimensions
└── streamlit/
    └── client_reporting_app.py            # Streamlit app (deployed in Part 3)
```

## Data Model

The lab generates synthetic asset-management data across six tables:

```
RAW.CLIENTS (30 rows)           RAW.SECURITIES (200 rows)
    │                               │
    └── RAW.PORTFOLIOS (100 rows)   │
            │                       │
            ├── RAW.HOLDINGS (5K rows) ──┘
            │       │
            │       └──> ANALYTICS.STG_HOLDINGS
            │                   │
            ├── RAW.TRANSACTIONS (10K rows)
            │       │
            │       └──> ANALYTICS.STG_TRANSACTIONS
            │
            └── RAW.BENCHMARKS (1.2K rows)
                        │
        ┌───────────────┘
        │
        └──> MARTS.F_POSITIONS_DAILY (fact)
             MARTS.F_PERFORMANCE_DAILY (fact)
             MARTS.D_PORTFOLIO (dimension)
             MARTS.D_CLIENT (dimension)
```

## Key Snowflake Documentation Links

| Topic | Link |
|-------|------|
| Snowflake Notebooks | https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks |
| dbt Projects on Snowflake | https://docs.snowflake.com/en/user-guide/ui-snowsight/dbt |
| Streamlit in Snowflake | https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit |
| Snowpark Python | https://docs.snowflake.com/en/developer-guide/snowpark/python/index |
| Horizon Catalog | https://docs.snowflake.com/en/user-guide/governance-horizon |
| Object Tagging | https://docs.snowflake.com/en/user-guide/object-tagging |
| Internal Marketplace | https://docs.snowflake.com/en/user-guide/data-marketplace-internal |
| ACCOUNT_USAGE Views | https://docs.snowflake.com/en/sql-reference/account-usage |
| Resource Monitors | https://docs.snowflake.com/en/user-guide/resource-monitors |
| Git Repository Integration | https://docs.snowflake.com/en/developer-guide/git/git-setting-up |
| OpenFlow (reference only) | https://docs.snowflake.com/en/user-guide/data-load-openflow |

## Context: DWS SnowCamp

This lab is part of the DWS SnowCamp 2-day training event. The narrative across both days covers:

1. **Discover & govern** data products with Horizon Catalog / Internal Marketplace
2. **Ingest** data using OpenFlow (presentation only -- not in this hands-on lab)
3. **Transform** data using dbt Projects on Snowflake
4. **Present** data through Streamlit and Tableau
5. **Govern and secure** the platform with monitoring and administration
6. **Accelerate** development with Cortex Code

This hands-on lab covers items 1, 3, 4, and 5 in a single interactive notebook.

## License

Internal use -- DWS SnowCamp training material.
