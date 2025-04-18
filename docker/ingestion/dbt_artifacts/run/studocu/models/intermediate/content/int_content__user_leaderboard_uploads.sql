

  create view "production"."intermediate"."int_content__user_leaderboard_uploads__dbt_tmp" as (
    with user_uploads as (
    select
        document.user_id,
        coalesce(count(*), 0) as uploads
    from
        "production"."production_studocu"."document" document
    where 1 = 1
        -- and date_trunc('month', published_at) = date_trunc('month', current_date)
        and document.published_at is not null
        and document.deleted_at is null
        and document.approved_at is not null
    group by
        1
)
select
    *
from
    user_uploads
  ) with no schema binding;
