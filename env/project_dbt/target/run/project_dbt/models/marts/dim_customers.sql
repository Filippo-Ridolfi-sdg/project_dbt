
  create or replace   view project_dbt.schema_dbt.dim_customers
  
  
  
  
  as (
    select * from project_dbt.schema_dbt.stg_project__customers
  );

