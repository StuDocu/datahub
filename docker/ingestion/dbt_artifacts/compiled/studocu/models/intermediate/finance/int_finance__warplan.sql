with comments as ( 
select 
    institution_id,
    trunc(created_at) as created_at,
    comment_subtype,
    case when comment_subtype = 'EB' then comment -- extra budget
        else NULL end as eb_comment, 
    case when comment_subtype = 'UB' then comment -- uni bucket
        else NULL end as ub_comment,
    case when comment_subtype = 'UC' then comment -- uni comment
        else NULL end as uc_comment
from "production"."production_studocu"."warplan_institution_comment"
where deleted_at is NULL
qualify rank() over (partition by institution_id, comment_subtype order by created_at desc) = 1
),

consolidated_query as 
(
Select
    -- Course doc stats

    coalesce(course_docs.institution_id,0) as institution_id,
    coalesce(course_docs.iname,'NULL') as institution_name,
    course_docs.level as institution_level,
    course_docs.stage  as institution_stage,
    coalesce(course_docs.cid,0) as country_id,
    coalesce(course_docs.cname,'NULL') as country_name,
    coalesce(c_tier.tier,5) as tier,  

    -- Uni bucket 
    case 
        when course_docs.stage = 0 and course_docs.population < 7000 then '0. Non Focus' 
        when course_docs.stage = 0 and (paid_docs_published+organic_docs) > 300 then '0. Grew Organically'
        when course_docs.course_total < 150 and course_docs.stage = 0 and course_docs.population >= 7000 then '0. Find Curricula'
        when course_docs.course_total >= 150 and course_docs.stage = 0 and course_docs.population > 7000 then '1. Ready for Launch'
        when course_docs.stage = 1 and (((paid_docs_published+organic_docs) < 1100 and paid_docs_published < 800) or paid_docs_published <950) then '2. Buy'
        when pe.total_docs_payout_extra_eur is not NULL then '3. Buy Extra'
        when course_docs.stage = 1 then '4. Boost or Wait'
            when course_docs.stage > 1 and (paid_docs_published+organic_docs) between 1000 and 1500 then '5. Tipped >1000'
            when course_docs.stage > 1 and (paid_docs_published+organic_docs) between 1500 and 2000 then '6. Tipped >1500'
            when course_docs.stage > 1 and (paid_docs_published+organic_docs) >= 2000 then '7. Tipped >2000'
            else '8. Other' end as bucket, 
    case 
        when len(cub.ub_comment) > 0 then cub.ub_comment
        else NULL end as uni_bucket,

    -- Course doc stats

    coalesce(course_docs.factor,0) as factor,
    coalesce(course_docs."population",0) as "population",
    coalesce(course_docs.course_total,0) as course_total,
    coalesce(course_docs.courses_docs,0) as courses_docs,
    coalesce(course_docs.organic_docs,0) as organic_docs,
    coalesce(course_docs.paid_docs_potential,0) as paid_docs_potential,
    coalesce(course_docs.paid_docs_published,0) as paid_docs_published,
    coalesce(course_docs.staff_docs_published,0) as staff_docs_published,
    coalesce(course_docs.organic_docs_jul17,0) as organic_docs_jul17,
    coalesce(course_docs.paid_docs_potential_jul17,0) as paid_docs_potential_jul17,
    coalesce(course_docs.paid_docs_published_jul17,0) as paid_docs_published_jul17,
    coalesce(course_docs.staff_docs_published_jul17,0) as staff_docs_published_jul17,

    -- Payouts & Payout extras
    case 
        when pt.total_doc_payout_eur is NULL then 0 
        else pt.total_doc_payout_eur end as docs_payout_extra_eur,
    case 
        when len(ceb.eb_comment) > 0 then ceb.eb_comment
        else NULL end as extra_budget,
    case 
        when course_docs.stage <= 1 and ceb.eb_comment is NULL and (950 - (paid_docs_published))*3 > 0 then ((950 - (paid_docs_published))*3)
        when course_docs.stage <= 1 and pe.total_docs_payout_extra_eur < 1.0*(ceb.eb_comment :: FLOAT) then (1.0*(ceb.eb_comment :: FLOAT) - (pe.total_docs_payout_extra_eur :: FLOAT))
        else 0 end as remaining_investment, 

    -- Uni stage change 
    case when ceb.eb_comment is not NULL and course_docs.stage = 1 and (coalesce(pe.total_docs_payout_extra_eur,0) :: FLOAT) > 0.8*1.0*(ceb.eb_comment:: FLOAT) then 'EB_to_EB_or_2'
        when ceb.eb_comment is NULL and course_docs.stage = 1 and (((paid_docs_published+organic_docs) >= 1100 and paid_docs_published >= 800) or paid_docs_published >=950) then '1_to_EB_or_2'
        else NULL end as status_changes,

    -- Institution Stats
    coalesce(institution_stats.days_phase1,0) as days_phase1 ,
    0 as docs_paid_L7D,
    0 as docs_paid_L30D,
    0 as docs_paid_L180D,
    coalesce(institution_stats.docs_total,0) as docs_total,
    coalesce(institution_stats.docs_paid_total,0) as docs_paid_total,
    coalesce(institution_stats.lecture_notes,0) as lecture_notes,
    coalesce(institution_stats.exams,0) as exams,
    coalesce(institution_stats.summaries,0) as summaries,
    coalesce(institution_stats.assignments,0) as assignments,
    coalesce(institution_stats.essays,0) as essays,
    coalesce(institution_stats.lecture_notes_perct,0) as lecture_notes_perct,
    coalesce(institution_stats.exams_perct,0) as exams_perct,
    coalesce(institution_stats.summaries_perct,0) as summaries_perct,
    coalesce(institution_stats.assignments_perct,0) as assignments_perct,
    coalesce(institution_stats.essays_perct,0) as essays_perct,
    coalesce(institution_stats.unique_uploaders,0) as unique_uploaders,
    coalesce(institution_stats.ratings_number,0) as ratings_number,
    coalesce(institution_stats.rating_score,0) as rating_score,
    coalesce(institution_stats.courses_total,0) as courses_total,
    coalesce(institution_stats.courses_above_5_docs,0) as courses_above_5_docs,
    coalesce(institution_stats.courses_above_10_docs,0) as courses_above_10_docs,
    coalesce(institution_stats.courses_above_20_docs,0) as courses_above_20_docs,
    coalesce(institution_stats.courses_above_5_docs_perct,0) as courses_above_5_docs_perct,
    coalesce(institution_stats.courses_above_10_docs_perct,0) as courses_above_10_docs_perct,
    coalesce(institution_stats.courses_above_20_docs_perct,0) as courses_above_20_docs_perct,

    -- Uni comments 
    case 
        when cuc.uc_comment is NULL then '' 
        else cuc.uc_comment end as uni_comment,

    -- DAU 
    coalesce(local_dau.max_local_daus_30d,0) as max_local_daus_30d,

    --Uploader 
    coalesce(uploaders.max_uploaders_l30d,0) as max_uploaders_l30d,

    --Document view
    coalesce(unique_doc_view.unique_dv_l90d,0) as unique_dv_l90d,
    coalesce(unique_doc_view.unique_dv_l90d_ly,0) as unique_dv_l90d_ly,
    coalesce(unique_doc_view.unique_dv_l90d_yoy,0) as unique_dv_l90d_yoy,

    -- Course view 
    coalesce(unique_course_view.unique_cv_l90d,0) as unique_cv_l90d,
    coalesce(unique_course_view.unique_cv_l90d_ly,0) as unique_cv_l90d_ly,
    coalesce(unique_course_view.unique_cv_l90d_yoy,0) as unique_cv_l90d_yoy,

    -- Sales 
    coalesce(sales.yearly_sales_eur,0) as yearly_sales_eur,
    coalesce(sales.yearly_sales_ly_eur,0) as yearly_sales_ly_eur,
    coalesce(sales.sales_yoy,0) as sales_yoy,
    coalesce(sales.subscribers,0) as subscribers,
    coalesce(sales.subscribers_ly,0) as subscribers_ly,
    coalesce(sales.subscribers_yoy,0) as subscribers_yoy
    
from  "production"."intermediate"."int_product__institution_course_docs" as course_docs
left join "raw_data"."general"."country_tier" as c_tier on c_tier.country_id = course_docs.cid
left join "production"."intermediate"."int_finance__institution_total_payout" as pt on pt.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_finance__institution_payout_extra" as pe on pe.institution_id = course_docs.institution_id
left join comments as ceb on ceb.institution_id = course_docs.institution_id and ceb.comment_subtype = 'EB'
left join comments as cub on cub.institution_id = course_docs.institution_id and cub.comment_subtype = 'UB'
left join comments as cuc on cuc.institution_id = course_docs.institution_id and cuc.comment_subtype = 'UC'
left join "production"."intermediate"."int_product__institution_stage1_stats" as institution_stats on institution_stats.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_product__institution_local_dau_30d" as local_dau on local_dau.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_product__institution_uploaders_30d" as uploaders on uploaders.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_product__unique_doc_view_l90d" as unique_doc_view on unique_doc_view.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_product__unique_course_view_l90d" as unique_course_view on unique_course_view.institution_id = course_docs.institution_id
left join "production"."intermediate"."int_finance__subscription_sales_yoy" as sales on sales.institution_id = course_docs.institution_id 
order by 
    1 
)

select * from consolidated_query order by 1