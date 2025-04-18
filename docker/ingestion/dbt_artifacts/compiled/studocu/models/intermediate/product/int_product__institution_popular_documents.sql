WITH l30d_document_ratings AS (
    SELECT
        document_rated.document_id,
        SUM(CASE WHEN document_rated = 'Positive' THEN 1 ELSE 0 END) as total_upvotes,
        SUM(CASE WHEN document_rated = 'Negative' THEN 1 ELSE 0 END) as total_downvotes
    FROM
        "production"."stg_mixpanel"."document_rated" document_rated
    INNER JOIN
        "production"."production_studocu"."document" document
        ON document_rated.document_id = document.id
    LEFT JOIN
        "production"."production_studocu"."course" course
        ON document.course_id = course.id
    WHERE
        DATE(event_datetime) >= DATEADD(day, -30, GETDATE())
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND document.no_index = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
    GROUP BY
        1
),
document_laplace AS (
    SELECT
        document.id as document_id,
        document.institution_id,
        document.rating_positive,
        document.rating_negative,
        CASE 
            WHEN total_upvotes IS NULL THEN 0.5
            ELSE 1.0* (1 + total_upvotes) / (2 + total_upvotes + total_downvotes) 
        END as l30d_laplace_score,
        1.0 * (1 + rating_positive) / (2 + rating_positive + rating_negative) as overall_laplace_score
    FROM 
        "production"."production_studocu"."document" document
    INNER JOIN
        "production"."production_studocu"."course" course
        ON document.course_id = course.id
    LEFT JOIN
        l30d_document_ratings
        ON document.id = l30d_document_ratings.document_id
    WHERE
        document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND document.no_index = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
),
institution_document_rank AS (
    SELECT
        document_id,
        institution_id,
        ROW_NUMBER() OVER (PARTITION BY institution_id ORDER BY l30d_laplace_score DESC, overall_laplace_score DESC, rating_positive DESC, rating_negative ASC, document_id DESC) as document_rank
    FROM
        document_laplace
)
SELECT
    'popular' as "type",
    'document' as model_type,
    document_id as model_id,
    institution_id
FROM
    institution_document_rank
WHERE   
    document_rank <= 300
ORDER BY
    4,
    3