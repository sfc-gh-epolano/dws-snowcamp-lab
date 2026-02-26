with portfolios as (
    select * from {{ source('raw', 'portfolios') }}
),

clients as (
    select * from {{ source('raw', 'clients') }}
)

select
    p.portfolio_id,
    p.portfolio_name,
    p.portfolio_type,
    p.benchmark_id,
    p.inception_date,
    p.client_id,
    c.client_name,
    c.region,
    c.aum_tier
from portfolios p
join clients c on p.client_id = c.client_id
