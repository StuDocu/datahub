

  create view "production"."intermediate"."int_content__user_leaderboard_impact__dbt_tmp" as (
    with total_downloads as (
    select
        document.user_id,
        coalesce(count(*),0) as impact
    from
        "production"."stg_mixpanel"."document_download" as download
    inner join
        "production"."production_studocu"."document" as document
        on download.document_id = document.id
    where
        date_trunc('month', download.event_datetime) = date_trunc('month', current_date)
        and document.published_at is not null
        and document.deleted_at is null
        and (download.user_id <> document.user_id or download.user_id is null) -- second condition keeps anonymous downloads
    group by
        1
)
select
    *
from
    total_downloads
  ) with no schema binding;
