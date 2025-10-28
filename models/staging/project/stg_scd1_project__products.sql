with 

source_t0 as ( 
    select * from {{ source('project', 'products_t0') }}
),

source_t1 as ( 
    select * from {{ source('project', 'products_t1') }}
),

select_t0 as (

    select 
        {{ dbt_utils.generate_surrogate_key([
            'PRODUCT_CD'
        ]) }} as product_id,

        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        cast(LIST_PRICE as numeric(10, 2)) as list_price,
        COLOR

    from source_t0
),

select_t1 as (

    select 
        {{ dbt_utils.generate_surrogate_key([
            'PRODUCT_CD'
        ]) }} as product_id,

        PRODUCT_CD,
        MODEL_NAME,
        BRAND,
        CATEGORY,
        cast(LIST_PRICE as numeric(10, 2)) as list_price,
        COLOR

    from source_t1
),

 final as (

    select
        coalesce(t1.product_id, t0.product_id) as product_id,
        coalesce(t1.product_cd, t0.product_cd) as product_cd,
        
        coalesce(t1.MODEL_NAME, t0.MODEL_NAME) as MODEL_NAME,
        coalesce(t1.BRAND, t0.BRAND) as BRAND, 
        coalesce(t1.CATEGORY, t0.CATEGORY) as CATEGORY, 
        coalesce(t1.list_price, t0.list_price) as list_price,
        coalesce(t1.COLOR, t0.COLOR) as COLOR,
        
        (t1.product_id is null) as is_deleted
    
    from select_t1 as t1
    
    full outer join select_t0 as t0
        on t1.product_id = t0.product_id
)

select * from final