with source as (
    select * from {{ source('raw', 'holdings') }}
),

cleaned as (
    select
        holding_id,
        portfolio_id,
        security_id,
        as_of_date,
        quantity,
        market_value,
        case
            when quantity > 0 then round(market_value / quantity, 4)
            else 0
        end as unit_price
    from source
    where market_value is not null
)

select * from cleaned
