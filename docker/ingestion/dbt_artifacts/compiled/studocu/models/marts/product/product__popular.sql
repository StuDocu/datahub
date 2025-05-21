WITH base AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__popular_institutions"
    UNION
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__popular_documents"
    UNION
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__popular_books"
    UNION
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__popular_topics"
    UNION
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__popular_courses"
)
SELECT
    current_date as process_date,
    *,
    
    
md5(cast(coalesce(cast(region_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(institution_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(type_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    base