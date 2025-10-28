
  
    

create or replace transient table project_dbt.schema_dbt.customers_scd
    
    
    
    as (

select *
from project_dbt.schema_dbt.stg_project__customers


    )
;


  