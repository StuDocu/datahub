WITH base AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__course_trending_documents_university"
    UNION ALL
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__course_trending_documents_high_school"
)
SELECT
    current_date as process_date,
    *,
    
    
md5(cast(coalesce(cast(course_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(document_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    base