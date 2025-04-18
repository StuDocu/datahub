
  
    

  create  table
    "production"."production_studocu"."role__dbt_tmp"
    
    
    
  as (
    WITH base AS (
    SELECT
        id,
        name,
        role_id,
        fixed
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."role"
    
  )

)
SELECT
    *
FROM
    base
  );
  