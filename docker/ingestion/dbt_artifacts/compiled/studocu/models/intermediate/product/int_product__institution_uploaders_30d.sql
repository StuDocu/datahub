with unique_uploaders as 
(   
    select 
        i.id as institution_id,
        cast(timezone('UTC',d.published_at) as date) as date_day,
        count(distinct d.user_id) as unique_uploaders
    from "production"."production_studocu"."document" d
    left join "production"."production_studocu"."course" c on d.course_id  = c.id 
    left join "production"."production_studocu"."institution" i on c.institution_id  = i.id 
    left join "production"."production_studocu"."country" ct on i.country_id = ct.id
    where cast(timezone('UTC',d.published_at) as date) between trunc(dateadd(day,-395, current_date)) and trunc(current_date) 
    group by 1,2
),

l30d_uploaders as
(select 
    t1.institution_id,
    t1.date_day,
    sum(t2.unique_uploaders) as uploaders_l30d
from unique_uploaders t1
inner join unique_uploaders t2 on t1.institution_id = t2.institution_id and ( t2.date_day between dateadd(day,-30, t1.date_day) and t1.date_day )
where t1.date_day between trunc(dateadd(day,-365,current_date)) and trunc(current_date)
group by 1,2
),

consolidated_uploaders as ( 
select 
    institution_id,
    max(uploaders_l30d) as max_uploaders_l30d
from l30d_uploaders
group by 1
order by 1 asc)

select * from consolidated_uploaders