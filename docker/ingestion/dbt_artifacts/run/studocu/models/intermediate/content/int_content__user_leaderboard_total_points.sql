

  create view "production"."intermediate"."int_content__user_leaderboard_total_points__dbt_tmp" as (
    with upvotes as (
    select
        *
    from
        "production"."intermediate"."int_content__user_leaderboard_upvotes" 
),
comments as (
    select
        *
    from
        "production"."intermediate"."int_content__user_leaderboard_comments" 
),
views as (
    select
        *
    from
        "production"."intermediate"."int_content__user_leaderboard_views" 
),
uploads as (
    select
        *
    from
        "production"."intermediate"."int_content__user_leaderboard_uploads" 
),
downloads as (
    select
        *
    from
        "production"."intermediate"."int_content__user_leaderboard_downloads" 
),
all_fields as (
    select
        upvotes.user_id,
        coalesce(upvotes.upvotes,0) as upvotes,
        coalesce(comments.comments,0) as comments,
        coalesce(views.views,0) as views,
        coalesce(downloads.downloads,0) as downloads,
        coalesce(uploads.uploads,0) as uploads
    from    
        upvotes
        left join comments on upvotes.user_id = comments.user_id
        left join views on upvotes.user_id = views.user_id
        left join uploads on upvotes.user_id = uploads.user_id
        left join downloads on upvotes.user_id = downloads.user_id
)
select 
    *,
    50*upvotes + 50*comments + 25*(views+downloads) + 10*uploads as total_points
from    
    all_fields
  ) with no schema binding;
