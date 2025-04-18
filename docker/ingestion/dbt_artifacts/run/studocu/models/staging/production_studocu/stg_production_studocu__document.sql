
  
    

  create  table
    "production"."production_studocu"."document__dbt_tmp"
    
    diststyle key distkey (id)
    
      compound sortkey(published_at)
  as (
    
WITH base AS (
    SELECT
        "id",
        "created_at",
        "updated_at",
        "deleted_at",
        "published_at",
        
    CONVERT_TIMEZONE(
        'UTC',
        'Europe/Amsterdam',
        TIMESTAMP 'epoch' + approved_at :: bigint * INTERVAL '1 second'
    )
 AS "approved_at",
        "lastmod_at",
        --"downloads",
        CASE 
            WHEN "premium_variants" IS NOT NULL THEN 1
            ELSE 0
        END as "is_premium",
        premium_variants,
        --"active",
        --"deleted",
        "anonymous",
        "finished",
        --"published",
        "stamp",
        "cover",
        "category_id",
        "course_id",
        "institution_id",
        "user_id",
        "book_id",
        coalesce(title, '') as title,
        "rating",
        "rating_positive",
        "rating_negative",
        --"popularity_score",
        "language",
        "detected_language",
        coalesce(description, '') as "description",
        --"article_name",
        "edited_by",
        "merged",
        --"naming_schema_id",
        "auto_approved",
        "new_title",
        "language_from_institution",
        --"semester",
        "auto_title",
        "auto_disapproved",
        "delete_reason_id",
        "reward",
        "needs_review",
        "manual_review",
        "user_accepted",
        "document_acquisition_type_id",
        -- "has_practice",
        --"has_terms",
        "no_index",
        "manual_review_reason",
        "additional_details",
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS row_num
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."document"
    
  )

)
SELECT
    *,
    
    
md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(updated_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(published_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(approved_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(course_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    base
WHERE 
    row_num = 1
  );
  