with 

source_t0 as ( 
    select * from {{ source('project', 'products_t0') }}
),

source_t1 as ( 
    select * from {{ source('project', 'products_t1') }}
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
        COLOR,
        
        True as is_valid

    from source_t1
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
        COLOR,

        False as is_valid

    from source_t0
),

unioned_data as (

    select * from select_t1
    
    union all
    
    select * from select_t0
),

final as (

    select
        *,
        row_number() over (
            partition by 
                product_id,
                MODEL_NAME,
                BRAND,
                CATEGORY,
                list_price,
                COLOR
            order by 
                is_valid desc 
        ) as rn
    
    from unioned_data
)

select
    product_id,
    PRODUCT_CD,
    MODEL_NAME,
    BRAND, 
    CATEGORY, 
    list_price,
    COLOR,
    is_valid
from final
where rn = 1