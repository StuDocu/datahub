WITH course_recency_rank AS (
    select 
        course.grade_id,
        course.id as course_id,
        ROW_NUMBER() OVER (PARTITION BY course.grade_id ORDER BY course.updated_at DESC) as course_rank
    from
        "production"."production_studocu"."course"
    where
        course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and course.degree_id is null
        and course.active = 1
        and course.deleted_at is null
        and course.user_count is not null
        and course.document_count > 0
)
SELECT
    'recent' as "type",
    'course' as model_type,
    course_id as model_id,
    grade_id
FROM
    course_recency_rank
WHERE   
    course_rank <= 40
ORDER BY
    4,
    3