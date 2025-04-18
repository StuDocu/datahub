with uploaders as (
    select distinct
        document.user_id,
        users.institution_id
    from
        "production"."production_studocu"."document" as document
    inner join
        "production"."production_studocu"."user" as users   
        on document.user_id = users.id
    where   
        user_id not in (0, 2604634) --user 0 is anonymous and 2604634 is unybook@studocu.com
)
select
    current_date as process_date,
    uploaders.user_id,
    uploaders.institution_id,
    coalesce(total_points.total_points,0) as total_points,
    coalesce(total_points.upvotes,0) as upvotes,
    coalesce(total_points.comments,0) as comments,
    coalesce(total_points.views,0) as views,
    coalesce(total_points.downloads,0) as downloads,
    coalesce(total_points.uploads,0) as uploads,
    courses.course_ids,
    
    
md5(cast(coalesce(cast(uploaders.user_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(uploaders.institution_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
from
    uploaders
left join
    "production"."intermediate"."int_content__user_leaderboard_total_points" as total_points
    on uploaders.user_id = total_points.user_id
left join
    "production"."intermediate"."int_content__user_leaderboard_courses" as courses
    on uploaders.user_id = courses.user_id