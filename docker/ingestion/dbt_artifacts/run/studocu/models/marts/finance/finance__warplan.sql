
  
    

  create  table
    "production"."finance"."warplan__dbt_tmp"
    
    
    
  as (
    WITH base AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_finance__warplan"
)
SELECT
    *
FROM
    base
  );
  