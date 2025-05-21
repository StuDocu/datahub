

  create view "production"."intermediate"."int_content__user_leaderboard_views__dbt_tmp" as (
    with user_views as (
    select
        document.user_id,
        coalesce(count(*), 0) as views
    from
        "production"."production_studocu"."document" document
    left join
        "production"."intermediate"."int_content__document_view_minimal" document_view
        on document.id = document_view.document_id
        and (document.user_id <> document_view.user_id or document_view.user_id is null) -- second condition keeps anonymous views
    where 1 = 1
      --  and date_trunc('month', document_view.event_datetime) = date_trunc('month', current_date)
        and document_view.event_datetime > current_date - 180
        and document.published_at is not null
        and document.deleted_at is null
        and (document_view.user_id <> document.user_id or document_view.user_id is null) -- second condition keeps anonymous downloads
    group by
        1
)
select
    *
from
    user_views
  ) with no schema binding;
