with holdings as (
    select * from {{ ref('stg_holdings') }}
),

securities as (
    select * from {{ ref('stg_securities') }}
),

portfolio_totals as (
    select
        portfolio_id,
        as_of_date,
        sum(market_value) as total_portfolio_value
    from holdings
    group by portfolio_id, as_of_date
),

joined as (
    select
        h.holding_id,
        h.portfolio_id,
        h.security_id,
        h.as_of_date,
        h.quantity,
        h.market_value,
        h.unit_price,
        s.isin,
        s.security_name,
        s.sector,
        s.asset_class,
        s.currency,
        pt.total_portfolio_value,
        case
            when pt.total_portfolio_value > 0
            then round(h.market_value / pt.total_portfolio_value, 6)
            else 0
        end as portfolio_weight
    from holdings h
    join securities s on h.security_id = s.security_id
    join portfolio_totals pt
        on h.portfolio_id = pt.portfolio_id
        and h.as_of_date = pt.as_of_date
)

select * from joined
