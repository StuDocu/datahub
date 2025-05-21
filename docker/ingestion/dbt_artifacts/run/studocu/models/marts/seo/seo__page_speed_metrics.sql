

  create view "production"."seo"."page_speed_metrics__dbt_tmp" as (
    WITH 
base AS (
    SELECT 
        *
    FROM 
        "production"."intermediate"."int_seo__page_speed_metrics"
)

SELECT 
    *
FROM 
    base
  ) ;
