with source as (
    select * from {{ source('cybersyn', 'stock_price_timeseries') }}
),

closing_prices as (
    select
        ticker,
        date           as price_date,
        value          as close_price
    from source
    where variable = 'post-market_close_adjusted'
      and value is not null
)

select * from closing_prices
