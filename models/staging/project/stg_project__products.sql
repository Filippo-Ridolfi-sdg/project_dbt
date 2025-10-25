with 

source as (

    select * from {{ source('project', 'products')}}
),

transformed as (

    select 
        product_id,
        model_name as product_model_name,
        brand as product_brand,
        category as product_category,
        list_price as product_list_price,
        color as product_color,
        current_timestamp() as loaded_at

    from source
)

select * from transformed