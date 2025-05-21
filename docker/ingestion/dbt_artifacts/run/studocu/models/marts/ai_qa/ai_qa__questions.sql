
  
    

  create  table
    "production"."ai_qa"."questions__dbt_tmp"
    
    
    
  as (
    WITH questions AS (
    SELECT
        *,
        
    
md5(cast(coalesce(cast(post_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(institution_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
    FROM
        "production"."intermediate"."questions"
)   
SELECT
    *
FROM 
    questions
  );
  