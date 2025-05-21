
  
    

  create  table
    "production"."intermediate"."int_product__institution_course_docs__dbt_tmp"
    
    
    
  as (
    with uni_course_docs as (
select
    ct.id as cid,
    ct.name as cname,
    i.id as institution_id,
    i.name as iname,
    i.phase as stage,
    i.population,
    i.level,
    i.paid_acquisition as factor,
    count(distinct c.id) as courses_docs,
    sum(case when d.document_acquisition_type_id = 1 then 1 else 0 end) as organic_docs,
    sum(case when d.document_acquisition_type_id = 2 then 1 else 0 end) as paid_docs_potential,
    sum(case when d.document_acquisition_type_id = 2 and d.published_at is NOT NULL then 1 else 0 end) as paid_docs_published,
    sum(case when d.document_acquisition_type_id = 3 and d.published_at is NOT NULL then 1 else 0 end) as staff_docs_published,
    sum(case when d.document_acquisition_type_id = 1 and d.created_at >= date '2017-01-01' then 1 else 0 end) as organic_docs_jul17,
    sum(case when d.document_acquisition_type_id = 2 and d.created_at >= date '2017-01-01' then 1 else 0 end) as paid_docs_potential_jul17,
    sum(case when d.document_acquisition_type_id = 2 and d.created_at >= date '2017-01-01' and d.published_at is NOT NULL then 1 else 0 end) as paid_docs_published_jul17,
    sum(case when d.document_acquisition_type_id = 3 and d.created_at >= date '2017-01-01' and d.published_at is NOT NULL then 1 else 0 end) as staff_docs_published_jul17

from "production"."production_studocu"."institution" i
left join "production"."production_studocu"."country" ct on ct.id = i.country_id
left join "production"."production_studocu"."course" c on i.id = c.institution_id
left join "production"."production_studocu"."document" d on d.course_id = c.id

where 
    d.deleted_at is NULL 
    and d.finished = 1
    and d.user_accepted = 1
    and i.active = 1 
    and i.deleted = 0
    and i.merged_into_id is NULL

group by 
    ct.id, 
    ct.name, 
    i.id,
    i.name, 
    i.phase, 
    i.population,
    i.level,
    i.paid_acquisition 
)

select 
    cid,
    cname,
    ucd.institution_id,
    iname,
    stage,
    factor,
    "population",
    "level",
    count(*) as course_total,
    courses_docs,
    organic_docs,
    paid_docs_potential,
    paid_docs_published,
    staff_docs_published,
    organic_docs_jul17,
    paid_docs_potential_jul17,
    paid_docs_published_jul17,
    staff_docs_published_jul17
from uni_course_docs ucd 
left join "production"."production_studocu"."course" c on c.institution_id = ucd.institution_id
group by
    cid,
    cname,
    ucd.institution_id,
    iname,
    stage,
    factor,
    "population",
    "level",
    courses_docs,
    organic_docs,
    paid_docs_potential,
    paid_docs_published,
    staff_docs_published,
    organic_docs_jul17,
    paid_docs_potential_jul17,
    paid_docs_published_jul17,
    staff_docs_published_jul17
order by 
    cid
  );
  