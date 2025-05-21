
  
    

  create  table
    "production"."intermediate"."int_product__institution_stage1_stats__dbt_tmp"
    
    
    
  as (
    with active_documents as 
(
    select  c.institution_id,        
            c.id,
            c.name,
            count(*) as active_documents
    from "production"."production_studocu"."course" c        
    left join "production"."production_studocu"."document" d on c.id = d.course_id        
    inner join "production"."production_studocu"."institution" i on i.id = c.institution_id        
    where c.active = 1        
    and d.published_at is not NULL 
    and d.deleted_at is NULL       
    and i.phase >= 1        
    group by        
        1, 2, 3
),

course_counts as 
(
    select            
        institution_id,        
        count(*) as courses_total,        
        sum(case when active_documents >= 5 then 1 else 0 end) as courses_above_5_docs,       
        sum(case when active_documents >= 10 then 1 else 0 end) as courses_above_10_docs,        
        sum(case when active_documents >= 20 then 1 else 0 end) as courses_above_20_docs        
    from active_documents dc1        
    group by institution_id
),

institution_started_date as 
(
select
    c.institution_id,
    min(case 
        when d.document_acquisition_type_id = 2 then trunc(d.created_at) 
        end) as inst_created_at
from "production"."production_studocu"."institution" i                
left join "production"."production_studocu"."course" c on c.institution_id = i.id                
left join "production"."production_studocu"."document" d on d.course_id = c.id
group by c.institution_id
),

stage_1_stats as (

select 
        i.id  as institution_id, 
        case when i.phase = 1 then DATEDIFF('day',bst1.inst_created_at,trunc(current_date)) end as days_phase1,        
        count(*) as docs_total,      
        sum(case when document_acquisition_type_id = 2 then 1 else 0 end) as docs_paid_total,        
        sum(case when d.category_id = 3 then 1 else 0 end) as lecture_notes,
        sum(case when d.category_id = 4 then 1 else 0 end) as exams,
        sum(case when d.category_id = 7 then 1 else 0 end) as summaries,
        sum(case when d.category_id = 8 then 1 else 0 end) as assignments,
        sum(case when d.category_id = 10 then 1 else 0 end) as essays,
        case when count(*) >0 then 1.0*sum(case when d.category_id = 3 then 1 else 0 end)/count(*) else 0 end as lecture_notes_perct,
        case when count(*) >0 then 1.0*sum(case when d.category_id = 4 then 1 else 0 end)/count(*) else 0 end as exams_perct,
        case when count(*) >0 then 1.0*sum(case when d.category_id = 7 then 1 else 0 end)/count(*) else 0 end as summaries_perct,
        case when count(*) >0 then 1.0*sum(case when d.category_id = 8 then 1 else 0 end)/count(*) else 0 end as assignments_perct,
        case when count(*) >0 then 1.0*sum(case when d.category_id = 10 then 1 else 0 end)/count(*) else 0 end as essays_perct,      
        count(distinct d.user_id) as unique_uploaders ,       
        sum(d.rating_positive + d.rating_negative) as ratings_number,        
        case when (sum(d.rating_positive)+ sum(d.rating_negative)) >0 then 1.0*sum(d.rating_positive) / (sum(d.rating_positive)+ sum(d.rating_negative)) else 0 end as rating_score,        
        dc.courses_total,        
        dc.courses_above_5_docs ,       
        dc.courses_above_10_docs ,       
        dc.courses_above_20_docs ,       
        case when dc.courses_total>0 then (1.0*dc.courses_above_5_docs)/dc.courses_total else 0 end as courses_above_5_docs_perct,
        case when dc.courses_total>0 then (1.0*dc.courses_above_10_docs)/dc.courses_total else 0 end as courses_above_10_docs_perct,    
        case when dc.courses_total>0 then (1.0*dc.courses_above_20_docs)/dc.courses_total else 0 end as courses_above_20_docs_perct            
from "production"."production_studocu"."institution" i                
left join "production"."production_studocu"."course" c on c.institution_id = i.id                
left join "production"."production_studocu"."document" d on d.course_id = c.id 
left join institution_started_date bst1 on bst1.institution_id = i.id
left join course_counts dc on dc.institution_id = i.id                            
where 
    i.phase >= 1 
    and i.active = 1 
    and i.deleted = 0 
    and i.merged_into_id is NULL                
    and d.published_at is not NULL  
    and d.deleted_at is NULL                
group by                
    i.id, 
    days_phase1,
    dc.courses_total,
    dc.courses_above_5_docs,        
    dc.courses_above_10_docs,        
    dc.courses_above_20_docs      
)

select * from stage_1_stats
  );
  