with source as (
    select * from {{ source('raw', 'securities') }}
),

cleaned as (
    select
        security_id,
        isin,
        security_name,
        sector,
        asset_class,
        currency
    from source
)

select * from cleaned
