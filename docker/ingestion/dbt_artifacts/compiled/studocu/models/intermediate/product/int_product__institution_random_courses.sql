WITH random_courses AS (
    SELECT
        course.id as course_id,
        course.institution_id,
        ROW_NUMBER() OVER (PARTITION BY course.institution_id ORDER BY RANDOM()) AS course_rank
    FROM
        "production"."production_studocu"."course" as course
    WHERE
        course.active = 1
        AND course.deleted_at IS NULL
        AND course.user_count IS NOT NULL
        AND course.document_count >= 5
)
SELECT
    'random' as "type",
    'course' as model_type,
    course_id as model_id,
    institution_id
FROM
    random_courses
WHERE
    course_rank <= 600
ORDER BY
    4,
    3