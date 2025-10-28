
  create or replace   view project_dbt.schema_dbt.stg_project__customers
  
  
  
  
  as (
    with 

source as (

    select * from project_dbt.project.customers
),

transformed as (

    select 
        customer_id,
        name as customer_name,
        email as customer_email,
        city as customer_city,
        member_since

    from source
)

select * from transformed
  );

