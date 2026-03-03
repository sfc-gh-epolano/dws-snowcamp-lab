with source as (
    select * from {{ source('cybersyn', 'stock_price_timeseries') }}
),

closing_prices as (
    select
        primary_ticker as ticker,
        date           as price_date,
        value          as close_price
    from source
    where variable_name = 'Post-Market Close'
      and value is not null
)

select * from closing_prices
