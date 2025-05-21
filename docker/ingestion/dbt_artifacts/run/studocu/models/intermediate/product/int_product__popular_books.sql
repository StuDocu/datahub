

  create view "production"."intermediate"."int_product__popular_books__dbt_tmp" as (
    WITH popular_books AS (
    SELECT
        institution.region_code,
        CASE WHEN institution.level = 3 OR institution.id IS NULL THEN 'high_school' ELSE 'university' END as "institution_type",
        'book' as "type",
        book.id as "type_id",
        COUNT(document.id) AS num_docs
    FROM
        "production"."production_studocu"."book" book
    INNER JOIN
        "production"."production_studocu"."document" document
        ON book.id = document.book_id
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
        AND book.active = 1
        AND book.deleted = 0
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
    GROUP BY
        1,
        2,
        3,
        4
),
popular_books_with_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region_code, institution_type ORDER BY num_docs DESC) AS rank_order
    FROM
        popular_books
)
SELECT
    region_code,
    institution_type,
    "type",
    "type_id",
    rank_order
FROM
    popular_books_with_rank
WHERE
    rank_order <= 60
ORDER BY
    1,
    2,
    3,
    5 ASC
  ) with no schema binding;
