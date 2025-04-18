with 
    institutions as (
        select 
            i.id as institution_id 
            , i.phase as stage
            , i.population
            , i.country_id
            , ct.tier
        from "production"."production_studocu"."institution" i
        join "raw_data"."general"."country_tier" ct on ct.country_id = i.country_id
    ), 
    traffic as (
        select 
            institution_id 
            , date_trunc('month', view_date) as dt
            , sum(total_document_views) as views 
            , sum(premium_document_views) as premium_views 
        from "production"."content"."document_view_institution_daily"
        where view_date >= '2020-01-01'
        group by 1, 2 
    ),
    revenue as (
        select 
            university_id as institution_id 
            , date_trunc('month', date) as dt
            , count(distinct user_id) as paying_subscribers
            , sum(daily_revenue_eur_excl_vat) as revenue_eur 
        from "production"."finance"."daily_recurring_revenue"
        where university_id is not null
            and date >= '2020-01-01'
        group by 1, 2 
    ), 
    supply as (
        select 
            institution_id 
            , date_trunc('month', published_at) as dt
            , count(distinct id) as published_documents
            , count(distinct user_id) as contributors
        from "production"."production_studocu"."document"
        where published_at >= '2020-01-01'
        group by 1, 2 
    ), 
    direct_trials as (
        select 
            d.institution_id
            , date_trunc('month', t.doc_view_datetime) as dt
            , count(*) as direct_trials
            , count(distinct t.document_id) as converting_documents
        from "production"."content"."last_doc_view_before_trial" t
        left join "production"."production_studocu"."document" d on d.id = t.document_id
        where t.doc_view_datetime >= '2020-01-01'
        group by 1, 2
    ), 


    everything_joined as (
        select
            t.institution_id 
            , i.stage
            , i.population
            , i.country_id
            , i.tier
            , t.dt 
            , t.views
            , t.premium_views
            , coalesce(r.paying_subscribers, 0) as paying_subscribers
            , coalesce(r.revenue_eur, 0) as revenue_eur
            , coalesce(s.published_documents, 0) as published_documents
            , coalesce(s.contributors, 0) as contributors
            , coalesce(d.direct_trials, 0) as direct_trials
            , coalesce(d.converting_documents, 0) as converting_documents
        from traffic t
        left join revenue r on r.institution_id = t.institution_id and r.dt = t.dt 
        left join supply s on s.institution_id = t.institution_id and s.dt = t.dt 
        left join direct_trials d on d.institution_id = t.institution_id and d.dt = t.dt 
        
        left join institutions i on i.institution_id = t.institution_id
        where i.institution_id is not null 
    )
    

select *
from everything_joined
order by institution_id, dt