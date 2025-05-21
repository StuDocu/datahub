WITH popular_docs AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_popular_documents"
),
recent_docs AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_recent_documents"
),
random_docs AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_random_documents"
),
popular_courses AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_popular_courses"
),
recent_courses AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_recent_courses"
),
random_courses AS (
    SELECT
        *
    FROM
        "production"."intermediate"."int_product__institution_random_courses"
),
union_tables AS (
    SELECT
        *
    FROM
        popular_docs
    UNION
    (
        SELECT
            *
        FROM
            recent_docs
        WHERE
            model_id NOT IN (
                SELECT
                    model_id
                FROM
                    popular_docs
            )
    )
    UNION
    (
        SELECT
            *
        FROM
            random_docs
        WHERE
            model_id NOT IN (
                SELECT
                    model_id
                FROM
                    popular_docs
                UNION
                SELECT
                    model_id
                FROM
                    recent_docs
            )
    )
    UNION 
    SELECT
        *
    FROM
        popular_courses
    UNION
    (
        SELECT
            *
        FROM
            recent_courses
        WHERE
            model_id NOT IN (
                SELECT
                    model_id
                FROM
                    popular_courses
            )
    )
    UNION
    (
        SELECT
            *
        FROM
            random_courses
        WHERE
            model_id NOT IN (
                SELECT
                    model_id
                FROM
                    popular_courses
                UNION
                SELECT
                    model_id
                FROM    
                    recent_courses
            )
    )
)        
SELECT
    current_date as process_date,
    *,
    
    
md5(cast(coalesce(cast(type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(model_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(model_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(institution_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    union_tables