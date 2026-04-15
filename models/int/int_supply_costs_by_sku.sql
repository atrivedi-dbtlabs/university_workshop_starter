with supplies as (

    select
        *
    from  {{ ref('stg_jaffle_shop__raw_supplies') }}

),

aggregated as (

    select
        sku,
        sum(supply_cost) as unit_cost,
        logical_or(is_perishable) as is_perishable,
        count(*) as supply_count
    from supplies
    group by 1

)

select *
from aggregated