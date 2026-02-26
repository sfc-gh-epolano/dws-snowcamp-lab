with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        portfolio_id,
        security_id,
        trade_date,
        side,
        quantity,
        price,
        amount,
        case side
            when 'BUY' then quantity
            when 'SELL' then -quantity
            else 0
        end as signed_quantity
    from source
    where price > 0
)

select * from cleaned
