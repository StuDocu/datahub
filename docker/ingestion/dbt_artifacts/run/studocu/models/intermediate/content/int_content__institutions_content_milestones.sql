

  create view "production"."intermediate"."int_content__institutions_content_milestones__dbt_tmp" as (
    -- Final version

with months_calendar as 
(
    select 
        distinct trunc(date_trunc('month',"date")) as months
    from 
        "production"."general"."calendar"
    where "date" <= current_date

),
institution_docs_count as 
(
    select
        i.id, 
        i.population,
        i.country_id,
        trunc(date_trunc('month', published_at)) as published_month,
        rank() over (partition by i.id order by d.published_at,d.id asc) as rk
    from "production"."production_studocu"."institution" i
    inner join "production"."production_studocu"."course" c on c.institution_id = i.id
    inner join "production"."production_studocu"."document" d on d.course_id = c.id
    where d.published_at is not null
),
institution_docs_milestone as
(
    select 
        * 
    from 
        institution_docs_count
    where   
        rk in (1000,2000,3000,4000,5000)
), 
months_range as 
(
    select 
        id as country_id,
        months
    from 
       "production"."production_studocu"."country"
    cross join 
        months_calendar
),
institution_count as 
(
    select 
       m.country_id,
       m.months, 
       sum(case when coalesce(rk,0) = 1000 then 1 else 0 end) as institution_count_1000_doc,
       sum(case when coalesce(rk,0) = 1000 then population else 0 end) as population_reached_1000_doc,
       sum(case when coalesce(rk,0) = 2000 then 1 else 0 end) as institution_count_2000_doc,
       sum(case when coalesce(rk,0) = 2000 then population else 0 end) as population_reached_2000_doc,
       sum(case when coalesce(rk,0) = 3000 then 1 else 0 end) as institution_count_3000_doc,
       sum(case when coalesce(rk,0) = 3000 then population else 0 end) as population_reached_3000_doc,
       sum(case when coalesce(rk,0) = 4000 then 1 else 0 end) as institution_count_4000_doc,
       sum(case when coalesce(rk,0) = 4000 then population else 0 end) as population_reached_4000_doc,
       sum(case when coalesce(rk,0) = 5000 then 1 else 0 end) as institution_count_5000_doc,
       sum(case when coalesce(rk,0) = 5000 then population else 0 end) as population_reached_5000_doc
    from
        months_range m
    left join
        institution_docs_milestone idm on idm.published_month = m.months and m.country_id = idm.country_id

    group by 
        1, 2
    order by 1, 2
),
cumulative_institution_count as 
(
    select 
        country_id,
        months, 
        sum(institution_count_1000_doc) over (partition by country_id order by months rows unbounded preceding) as institution_count_1000_doc,
        sum(population_reached_1000_doc) over (partition by country_id order by months rows unbounded preceding) as population_reached_1000_doc,
        sum(institution_count_2000_doc) over (partition by country_id order by months rows unbounded preceding) as institution_count_2000_doc,
        sum(population_reached_2000_doc) over (partition by country_id order by months rows unbounded preceding) as population_reached_2000_doc,
        sum(institution_count_3000_doc) over (partition by country_id order by months rows unbounded preceding) as institution_count_3000_doc,
        sum(population_reached_3000_doc) over (partition by country_id order by months rows unbounded preceding) as population_reached_3000_doc,
        sum(institution_count_4000_doc) over (partition by country_id order by months rows unbounded preceding) as institution_count_4000_doc,
        sum(population_reached_4000_doc) over (partition by country_id order by months rows unbounded preceding) as population_reached_4000_doc,
        sum(institution_count_5000_doc) over (partition by country_id order by months rows unbounded preceding) as institution_count_5000_doc,
        sum(population_reached_5000_doc) over (partition by country_id order by months rows unbounded preceding) as  population_reached_5000_doc

    from 
        institution_count
)

select
    geocode,
    months,
    case when lag(institution_count_1000_doc,12) over (partition by country_id order by months) is not null 
        then (institution_count_1000_doc - lag(institution_count_1000_doc,12) over (partition by country_id order by months))
        else institution_count_1000_doc end as kickstarted_institutions_count_L12M, 
    institution_count_1000_doc,
    institution_count_2000_doc,
    institution_count_3000_doc,
    institution_count_4000_doc,
    institution_count_5000_doc,
    population_reached_1000_doc, 
    population_reached_2000_doc,
    population_reached_3000_doc, 
    population_reached_4000_doc,
    population_reached_5000_doc
    
from 
    cumulative_institution_count
left join 
    "production"."production_studocu"."country" as country on country.id = country_id
order by 1, 2
  ) with no schema binding;
