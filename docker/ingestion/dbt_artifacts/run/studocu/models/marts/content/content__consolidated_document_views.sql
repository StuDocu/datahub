
  
    

  create  table
    "production"."content"."consolidated_document_views__dbt_tmp"
    
    
    
  as (
    select  
    d.id as document_id, 
    max(d.published_at) as published_at,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 7 then 1 end),0) as views_first_7_days,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 14 then 1 end),0) as views_first_14_days,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 30 then 1 end),0) as views_first_30_days,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 90 then 1 end),0) as views_first_90_days,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 180 then 1 end),0) as views_first_180_days,
    coalesce(sum(case when datediff(days, d.published_at , dv.event_datetime) <= 360 then 1 end),0) as views_first_360_days,
    count(*) as total_views,
    max(dv.event_datetime) as latest_doc_view_timestamp
from 
    "production"."production_studocu"."document" as d
left join 
    "production"."stg_mixpanel"."document_view" as dv on d.id = dv.document_id
where 
    d.user_id <> dv.user_id and 
    d.published_at is not null
group by 1
  );
  