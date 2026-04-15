with items as (

    select
        *
    from {{ ref('stg_jaffle_shop__raw_items') }}

),

orders as (

    select
        *
    from {{ ref('stg_jaffle_shop__raw_orders') }}

),

products as (

    select
        *
    from {{ ref('stg_jaffle_shop__raw_products') }}

),

sku_costs as (

    select
        *
    from {{ ref('int_supply_costs_by_sku') }}

),

stores as (

    select
        *
    from {{ ref('stg_jaffle_shop__raw_stores') }}

),

joined as (

    select
        --keys
        i.item_id,
        i.order_id,
        i.sku,
        --order context
        o.customer_id,
        o.store_id,
        o.ordered_at,
        o.order_date,
        --product attributes
        p.product_name,
        p.product_type,
        p.product_description,
        --store attributes
        s.store_name,
        s.opened_at as store_opened_at,
        s.tax_rate,
        --cost basis / supply attributes
        c.supply_count,
        c.is_perishable,
        --unit economics
        cast(1 as int64) as units_sold,
        p.product_price as unit_revenue,
        c.unit_cost as unit_cost,
        p.product_price - c.unit_cost as unit_margin,
        safe_divide(
            p.product_price - c.unit_cost,
            nullif(p.product_price, 0)
        ) as unit_margin_pct
    from items i
    join orders o
        on i.order_id = o.order_id
    left join products p
        on i.sku = p.sku
    left join sku_costs c
        on i.sku = c.sku
    left join stores s
        on o.store_id = s.store_id

)

select *
from joined