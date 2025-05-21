
  
    

  create  table
    "production"."production_studocu"."follow__dbt_tmp"
    
    
    
  as (
    WITH base AS (
    SELECT
        id,
        user_id,
        followable_type,
        followable_id,
        created_at,
        deleted_at,
        origin,
        is_auto
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."follow"
    
  )

)
SELECT
    *,
    
    
md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(followable_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(followable_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(deleted_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(origin as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(is_auto as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    base
  );
  