
  create or replace   view project_dbt.schema_dbt.dim_products
  
  
  
  
  as (
    select * from project_dbt.schema_dbt.stg_project__products
  );

