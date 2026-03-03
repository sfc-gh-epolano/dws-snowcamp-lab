with positions as (
    select * from {{ ref('int_portfolio_positions') }}
),

portfolios as (
    select
        portfolio_id,
        client_id,
        portfolio_name,
        portfolio_type,
        benchmark_id
    from {{ source('raw', 'portfolios') }}
)

select
    p.holding_id,
    p.portfolio_id,
    po.client_id,
    po.portfolio_name,
    po.portfolio_type,
    po.benchmark_id,
    p.security_id,
    p.ticker,
    p.isin,
    p.security_name,
    p.sector,
    p.asset_class,
    p.currency,
    p.as_of_date,
    p.quantity,
    p.market_value,
    p.unit_price,
    p.total_portfolio_value,
    p.portfolio_weight
from positions p
join portfolios po on p.portfolio_id = po.portfolio_id
