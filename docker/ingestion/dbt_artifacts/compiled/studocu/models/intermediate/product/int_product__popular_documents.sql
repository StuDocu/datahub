WITH popular_documents_with_rank AS (
    SELECT
        institution.region_code,
        CASE WHEN institution.level = 3 OR institution.id IS NULL THEN 'high_school' ELSE 'university' END as "institution_type",
        document.id as "type_id",
        ROW_NUMBER() OVER (PARTITION BY institution.region_code, institution_type ORDER BY 1.0 * (rating_positive + 1)/(rating_positive + rating_negative + 2) DESC, 
                           rating_positive DESC, rating_negative ASC, published_at DESC) as rank_order
    FROM
        "production"."production_studocu"."document" document
    LEFT JOIN
        "production"."production_studocu"."course" course
        ON document.course_id = course.id
    LEFT JOIN
        "production"."production_studocu"."institution" institution
        ON COALESCE(document.institution_id, course.institution_id) = institution.id
    WHERE
        institution.active = 1
        AND institution.deleted = 0
        AND course.deleted_at IS NULL
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
)
SELECT
    region_code,
    institution_type,
    'document' as "type",
    "type_id",
    rank_order
FROM
    popular_documents_with_rank
WHERE
    rank_order <= 60
ORDER BY
    1,
    2,
    3,
    5 ASC