WITH popular_institutions AS (
    SELECT
        region_code,
        CASE WHEN institution.level = 3 OR institution.id IS NULL THEN 'high_school' ELSE 'university' END as "institution_type",
        'institution' as "type",
        institution_id as "type_id",
        COUNT(document.id) as num_docs
    FROM
        "production"."production_studocu"."institution" institution
    INNER JOIN
        "production"."production_studocu"."document" document
        ON document.institution_id = institution.id
    WHERE
        institution.active = 1
        AND institution.deleted = 0
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
    GROUP BY
        1,
        2,
        3,
        4
),
popular_institutions_with_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region_code, institution_type ORDER BY num_docs DESC) as rank_order
    FROM
        popular_institutions
)
SELECT
    region_code,
    institution_type,
    "type",
    "type_id",
    rank_order
FROM
    popular_institutions_with_rank
WHERE
    rank_order <= 50
ORDER BY
    1,
    2,
    3,
    5 ASC