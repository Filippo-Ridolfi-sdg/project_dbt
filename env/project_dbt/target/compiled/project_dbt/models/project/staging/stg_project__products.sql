with 

source as (

    select * from project_dbt.project.products
),

transformed as (

    select 
        product_id,
        model_name as product_model_name,
        brand as product_brand,
        category as product_category,
        list_price as product_list_price,
        color as product_color

    from source
)

select * from transformed