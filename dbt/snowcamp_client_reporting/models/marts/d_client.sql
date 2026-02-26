with source as (
    select * from {{ source('raw', 'clients') }}
)

select
    client_id,
    client_name,
    region,
    aum_tier,
    onboarding_date
from source
