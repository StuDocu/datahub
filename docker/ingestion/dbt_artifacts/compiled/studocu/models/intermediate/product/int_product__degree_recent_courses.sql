WITH course_recency_rank AS (
    select 
        coalesce(course.degree_id, degree.id) as degree_id,
        course.id as course_id,
        ROW_NUMBER() OVER (PARTITION BY coalesce(course.degree_id, degree.id) ORDER BY course.updated_at DESC) as course_rank
    from
        "production"."production_studocu"."course"
    left join
        "production"."production_studocu"."grade"
        on course.grade_id = grade.id
    left join
        "production"."production_studocu"."degree"
        on grade.degree_id = degree.id
    where
        course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and coalesce(course.degree_id, degree.id) is not null
        and course.active = 1
        and course.deleted_at is null
        and course.user_count is not null
        and course.document_count > 0
)
SELECT
    'recent' as "type",
    'course' as model_type,
    course_id as model_id,
    degree_id
FROM
    course_recency_rank
WHERE   
    course_rank <= 40
ORDER BY
    4,
    3