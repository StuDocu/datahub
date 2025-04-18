WITH document_random_rank AS (
    select 
        document.id as document_id,
        coalesce(course.degree_id, degree.id) as degree_id,
        ROW_NUMBER() OVER (PARTITION BY coalesce(course.degree_id, degree.id) ORDER BY RANDOM()) as document_rank
    from
        "production"."production_studocu"."document"
    inner join 
        "production"."production_studocu"."course"
        on document.course_id = course.id
    left join
        "production"."production_studocu"."grade"
        on course.grade_id = grade.id
    left join
        "production"."production_studocu"."degree"
        on grade.degree_id = degree.id
    where
        document.published_at is not null
        and document.deleted_at is null
        and course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and document.no_index = 0
        and coalesce(course.degree_id, degree.id) is not null
        and course.active = 1
        and course.deleted_at is null
)
SELECT
    'random' as "type",
    'document' as model_type,
    document_id as model_id,
    degree_id
FROM
    document_random_rank
WHERE
    document_rank <= 300
ORDER BY
    4,
    3