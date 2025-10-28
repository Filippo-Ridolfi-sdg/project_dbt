
  create or replace   view project_dbt.schema_dbt.stg_project__products
  
  
  
  
  as (
    with 

source_t0 as (

    select * from project_dbt.schema_dbt.products
),

source_t1 as (
    select * from project_dbt.schema_dbt.products_t1
), 

transformed as (

    select
        coalesce(source_t1.product_id, source_t0.product_id) as product_id,
        coalesce(source_t1.model_name, source_t0.model_name) as product_model_name,
        coalesce(source_t1.brand, source_t0.brand) as product_brand,
        coalesce(source_t1.category, source_t0.category) as product_category,
        coalesce(source_t1.list_price, source_t0.list_price) as product_list_price,
        coalesce(source_t1.color, source_t0.color) as product_color,
        case
            when source_t0.product_id is null then 'NEW'
            when source_t1.product_id is null then 'DELETED'
            when (source_t1.model_name <> source_t0.model_name)
              or (source_t1.brand <> source_t0.brand)
              or (source_t1.category <> source_t0.category)
              or (source_t1.list_price <> source_t0.list_price)
              or (source_t1.color <> source_t0.color)
                then 'UPDATED'
            else 'UNCHANGED'
        end as state_product
    from source_t0
    full outer join source_t1 using (product_id)
)

select * from transformed
  );

