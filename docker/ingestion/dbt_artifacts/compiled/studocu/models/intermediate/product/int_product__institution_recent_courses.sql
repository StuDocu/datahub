WITH course_recency_rank AS (
    SELECT 
        course.id as course_id,
        course.institution_id,
        ROW_NUMBER() OVER (PARTITION BY course.institution_id ORDER BY course.updated_at DESC) as course_rank
    FROM
        "production"."production_studocu"."course" as course
    WHERE
        course.active = 1
        AND course.deleted_at IS NULL
        AND course.document_count > 0
        AND course.user_count IS NOT NULL
)
SELECT
    'recent' as "type",
    'course' as model_type,
    course_id as model_id,
    institution_id
FROM
    course_recency_rank
WHERE   
    course_rank <= 400
ORDER BY
    4,
    3