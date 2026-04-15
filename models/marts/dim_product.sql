with products as (

    select *
    from {{ ref('stg_jaffle_shop__raw_products') }}

),

sku_costs as (

    select *
    from  {{ ref('int_supply_costs_by_sku') }}

),

joined as (

    select
        p.sku,
        p.product_name,
        p.product_type,
        p.product_price,
        p.product_description,
        
        --supply side attributes (1:1 per sku)
        c.unit_cost,
        c.is_perishable,
        c.supply_count,

        --unit economics
        p.product_price - c.unit_cost as unit_margin,
        safe_divide(
            p.product_price - c.unit_cost,
            nullif(p.product_price, 0)
        ) as unit_margin_pct
    from products p
    left join sku_costs c
        using (sku)

)

select *
from joined