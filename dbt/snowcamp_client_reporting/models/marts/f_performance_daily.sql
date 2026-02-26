with portfolio_values as (
    select
        portfolio_id,
        as_of_date,
        sum(market_value) as total_market_value
    from {{ ref('stg_holdings') }}
    group by portfolio_id, as_of_date
),

portfolio_returns as (
    select
        portfolio_id,
        as_of_date,
        total_market_value,
        lag(total_market_value) over (
            partition by portfolio_id order by as_of_date
        ) as prev_market_value,
        case
            when lag(total_market_value) over (
                partition by portfolio_id order by as_of_date
            ) > 0
            then round(
                (total_market_value - lag(total_market_value) over (
                    partition by portfolio_id order by as_of_date
                )) / lag(total_market_value) over (
                    partition by portfolio_id order by as_of_date
                ), 6)
            else 0
        end as daily_return
    from portfolio_values
),

portfolios as (
    select portfolio_id, client_id, benchmark_id
    from {{ source('raw', 'portfolios') }}
),

benchmarks as (
    select benchmark_id, as_of_date, daily_return as benchmark_return
    from {{ source('raw', 'benchmarks') }}
)

select
    pr.portfolio_id,
    p.client_id,
    p.benchmark_id,
    pr.as_of_date,
    pr.total_market_value,
    pr.daily_return as portfolio_return,
    coalesce(b.benchmark_return, 0) as benchmark_return,
    round(pr.daily_return - coalesce(b.benchmark_return, 0), 6) as active_return
from portfolio_returns pr
join portfolios p on pr.portfolio_id = p.portfolio_id
left join benchmarks b
    on p.benchmark_id = b.benchmark_id
    and pr.as_of_date = b.as_of_date
where pr.prev_market_value is not null
