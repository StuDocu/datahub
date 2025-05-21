with user_upvotes as (
    select
        document.user_id,
        coalesce(sum(rating.rating), 0) as upvotes
    from
        "production"."production_studocu"."document" document
    left join
        "production"."production_studocu"."rating" rating
        on rating.document_id = document.id
    where 1 = 1 
        -- and date_trunc('month', rating.created_at) = date_trunc('month', current_date)
        and document.published_at is not null
        and document.deleted_at is null
        and (document.user_id <> rating.user_id or rating.user_id is null) -- second condition keeps anonymous ratings
    group by
        1
)
select
    *
from
    user_upvotes