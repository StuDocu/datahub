with doc_courses as (
    select distinct
        user_id,
        course_id
    from
        "production"."production_studocu"."document"
    where
        published_at is not null
        and deleted_at is null
),
course_follows as (
    select distinct
        course_user.user_id,
        course_user.course_id
    from
        "production"."production_studocu"."course_user"
    inner join
        doc_courses
        on doc_courses.user_id = course_user.user_id
    where
        is_auto = 0
),
all_courses as (
    select
        *
    from
        course_follows
    union -- union removes duplicates
    select
        *
    from
        doc_courses
)
select
    user_id,
    listagg(course_id, '|' ) within group (order by course_id) as course_ids
from
    all_courses
where 
    user_id <> 0
group by
    1