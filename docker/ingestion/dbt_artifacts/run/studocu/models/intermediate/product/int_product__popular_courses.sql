

  create view "production"."intermediate"."int_product__popular_courses__dbt_tmp" as (
    WITH popular_courses AS (
    SELECT
        institution.region_code,
        CASE WHEN institution.level = 3 OR institution.id IS NULL THEN 'high_school' ELSE 'university' END AS institution_type,
        'course' AS "type",
        course.id AS "type_id",
        COUNT(document_view.document_id) AS num_doc_views
    FROM
        "production"."production_studocu"."course" course
    LEFT JOIN
        "production"."intermediate"."int_content__document_view_minimal" document_view
        ON course.id = document_view.course_id
    LEFT JOIN
        "production"."production_studocu"."document" document
        ON document_view.document_id = document.id
    LEFT JOIN
        "production"."production_studocu"."institution" institution
        ON COALESCE(document.institution_id, course.institution_id) = institution.id
    WHERE
        institution.active = 1
        AND institution.deleted = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND institution.region_code IN ('nl', 'en-us', 'en-au', 'en-ca', 'es', 'en-gb', 'pt')  -- Filtering for specified regions
        and document_view.event_datetime >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY
        1, 2, 3, 4
),
popular_courses_with_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region_code, institution_type ORDER BY num_doc_views DESC) AS rank_order
    FROM
        popular_courses
)
SELECT
    region_code,
    institution_type,
    "type",
    "type_id",
    rank_order
FROM
    popular_courses_with_rank
WHERE
    rank_order <= 16
ORDER BY
    1,
    2,
    3,
    5 ASC
  ) with no schema binding;
