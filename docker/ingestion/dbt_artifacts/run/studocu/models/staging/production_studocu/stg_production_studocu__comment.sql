
  
    

  create  table
    "production"."production_studocu"."comment__dbt_tmp"
    
    diststyle key distkey (id)
    
  as (
    
WITH base AS (
    SELECT
        "id",
        coalesce("comment", '') AS "comment",
        "is_anonymous",
        "is_pinned",
        "is_expert_answer",
        "user_id",
        "version",
        "category",
        "created_at",
        "updated_at",
        "deleted_at"
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."comment"
    
  )

)
SELECT
    *
FROM
    base
  );
  