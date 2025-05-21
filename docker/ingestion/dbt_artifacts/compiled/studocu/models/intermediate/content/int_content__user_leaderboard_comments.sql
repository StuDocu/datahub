with user_comments as (
    select
        document.user_id,
        coalesce(count(distinct comment.id), 0) as comments
    from
        "production"."production_studocu"."document" document
    left join
        "production"."production_studocu"."comment_subject" comment_subject
        on document.id = comment_subject.subject_id
        and subject_type = 'Document'
    left join
        "production"."production_studocu"."comment" comment
        on comment_subject.comment_id = comment.id
        and (comment.user_id <> document.user_id or comment.user_id is null) -- second condition keeps anonymous comments
    where 1 = 1 
        -- and date_trunc('month', comment.created_at) = date_trunc('month', current_date)
        and document.published_at is not null
        and document.deleted_at is null
    group by
        1
)
select
    *
from
    user_comments