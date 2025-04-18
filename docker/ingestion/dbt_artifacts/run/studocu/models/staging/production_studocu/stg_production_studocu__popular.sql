
  
    

  create  table
    "production"."production_studocu"."popular__dbt_tmp"
    
    
    
  as (
    WITH base AS (
    SELECT
        "id",
        "language_id",
        "region_code",
        "institution_type",
        "type",
        "type_id",
        "rank_order"
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."popular"
    
  )

)
SELECT
    *
FROM
    base
  );
  