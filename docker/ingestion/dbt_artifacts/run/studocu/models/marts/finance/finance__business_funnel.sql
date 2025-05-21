
  
    

  create  table
    "production"."finance"."business_funnel__dbt_tmp"
    
    
    
  as (
    with base as  (
    select *
    from "production"."intermediate"."int_finance__business_funnel"
)

select *
from base
  );
  