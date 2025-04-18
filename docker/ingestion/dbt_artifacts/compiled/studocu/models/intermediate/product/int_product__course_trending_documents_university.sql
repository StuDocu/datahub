with document_views as (
    select
        document.course_id,
        docview.document_id,
        count(*) as l14d_views
    from
        "production"."stg_mixpanel"."document_view" as docview
    inner join
        "production"."production_studocu"."document" as document
        on docview.document_id = document.id
    inner join
        "production"."production_studocu"."institution" as document_institution
        on document.institution_id = document_institution.id
    inner join
        "production"."production_studocu"."user" as the_user
        on docview.user_id = the_user.id
        and the_user.institution_id = document.institution_id
    where
        docview.user_id is not null
        and docview.event_datetime::date >= date_add('day', -14, current_date)
        and document_institution.level <> 3
        and document.published_at is not null
        and document.deleted_at is null
    group by
        1,
        2
)
select
    *
from
    document_views