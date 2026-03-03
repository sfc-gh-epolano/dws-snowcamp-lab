with positions as (
    select * from {{ ref('f_positions_daily') }}
),

market_prices as (
    select * from {{ ref('stg_market_prices') }}
)

select
    p.holding_id,
    p.portfolio_id,
    p.client_id,
    p.portfolio_name,
    p.portfolio_type,
    p.security_id,
    p.ticker,
    p.security_name,
    p.sector,
    p.asset_class,
    p.currency,
    p.as_of_date,
    p.quantity,
    p.market_value     as synthetic_market_value,
    mkt.close_price    as market_close_price,
    case
        when mkt.close_price is not null
        then round(p.quantity * mkt.close_price, 2)
        else p.market_value
    end as mark_to_market_value,
    case
        when mkt.close_price is not null and p.market_value > 0
        then round((p.quantity * mkt.close_price - p.market_value) / p.market_value, 4)
        else 0
    end as valuation_diff_pct
from positions p
left join market_prices mkt
    on p.ticker = mkt.ticker
    and p.as_of_date = mkt.price_date
