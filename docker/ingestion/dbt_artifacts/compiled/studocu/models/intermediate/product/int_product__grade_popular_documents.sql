WITH relevant_documents AS (
    select 
        document.id as document_id,
        course.grade_id,
        rating_positive,
        rating_negative
    from
        "production"."production_studocu"."document"
    inner join 
        "production"."production_studocu"."course"
        on document.course_id = course.id
    where
        document.published_at is not null
        and document.deleted_at is null
        and course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and course.degree_id is null
        and document.no_index = 0
        and course.active = 1
        and course.deleted_at is null
),
l30d_document_ratings AS (
    SELECT
        document_rated.document_id,
        SUM(CASE WHEN document_rated = 'Positive' THEN 1 ELSE 0 END) as total_upvotes,
        SUM(CASE WHEN document_rated = 'Negative' THEN 1 ELSE 0 END) as total_downvotes
    FROM
        "production"."stg_mixpanel"."document_rated" document_rated
    INNER JOIN
        relevant_documents
        ON document_rated.document_id = relevant_documents.document_id
    WHERE
        DATE(event_datetime) >= DATEADD(day, -30, GETDATE())
    GROUP BY
        1
),
document_laplace AS (
    SELECT
        relevant_documents.document_id,
        relevant_documents.grade_id,
        relevant_documents.rating_positive,
        relevant_documents.rating_negative,
        CASE 
            WHEN total_upvotes IS NULL THEN 0.5
            ELSE 1.0* (1 + total_upvotes) / (2 + total_upvotes + total_downvotes) 
        END as l30d_laplace_score,
        1.0 * (1 + rating_positive) / (2 + rating_positive + rating_negative) as overall_laplace_score
    FROM 
        relevant_documents
    LEFT JOIN
        l30d_document_ratings
        ON relevant_documents.document_id = l30d_document_ratings.document_id
),
grade_document_rank AS (
    SELECT
        document_id,
        grade_id,
        ROW_NUMBER() OVER (PARTITION BY grade_id ORDER BY l30d_laplace_score DESC, overall_laplace_score DESC, rating_positive DESC, rating_negative ASC, document_id DESC) as document_rank
    FROM
        document_laplace
)
SELECT
    'popular' as "type",
    'document' as model_type,
    document_id as model_id,
    grade_id
FROM
    grade_document_rank
WHERE   
    document_rank <= 100
ORDER BY
    4,
    3