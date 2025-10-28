

select *
from project_dbt.schema_dbt.stg_project__customers



  -- "max(updated_at)" è il 't0'
  -- dbt processa solo i record con 'updated_at' > 't0'
  where updated_at > (select max(updated_at) from project_dbt.schema_dbt.customers_scd)

