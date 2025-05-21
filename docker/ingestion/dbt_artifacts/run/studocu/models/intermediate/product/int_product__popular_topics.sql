

  create view "production"."intermediate"."int_product__popular_topics__dbt_tmp" as (
    WITH popular_topics AS (
    SELECT
        course.region_code,
        'high_school' as "institution_type",
        'topic' as "type",
        topic_id as "type_id",
        COUNT(*) AS num_docs
    FROM
        "production"."production_studocu"."athena_document_topic" topic
    INNER JOIN
        "production"."production_studocu"."course" course
        ON topic.course_id = course.id
    INNER JOIN
        "production"."production_studocu"."document" document
        ON topic.document_id = document.id
    WHERE course.deleted_at IS NULL
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND topic.similarity_score >= '0.85'
    GROUP BY
        1,
        2,
        3,
        4
),
popular_topics_with_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region_code, institution_type ORDER BY num_docs DESC) AS rank_order
    FROM
        popular_topics
)
SELECT
    region_code,
    institution_type,
    "type",
    "type_id",
    rank_order
FROM
    popular_topics_with_rank
WHERE
    rank_order <= 15
ORDER BY
    1,
    2,
    3,
    5 ASC
  ) with no schema binding;
