
  
    

  create  table
    "production"."intermediate"."int_content__document_metadata__dbt_tmp"
    
    
    
  as (
    with payouts as (
    select
        ab.acquisition_batch_group_id,
        p.amount_eur as sum_payout_eur,
        count(distinct dab.document_id) as count_document_id,
        1.0*p.amount_eur/nullif(count(distinct dab.document_id),0) as payout_per_doc
    from "production"."production_studocu"."document_acquisition_batch" dab
    left join "production"."production_studocu"."acquisition_batch" ab
        on dab.acquisition_batch_id = ab.id
    left join "production"."production_studocu"."acquisition_batch_payout" abp
        on abp.acquisition_batch_id = ab.id
    left join "production"."production_studocu"."payout" p
        on abp.payout_id = p.id
    left join "production"."production_studocu"."document" d
        on dab.document_id = d.id
    where true
        and p.status = 'SUCCESS'
        and d.published_at is not null
    group by 1,2
),

payouts_doc as (
    select
        dab.document_id,
        p.*
    from payouts p
    left join "production"."production_studocu"."acquisition_batch" ab
        on ab.acquisition_batch_group_id = p.acquisition_batch_group_id
    left join "production"."production_studocu"."document_acquisition_batch" dab
        on dab.acquisition_batch_id = ab.id
)



SELECT
    document.id as document_id,
    max(document.title) as document_title,
    max(document.description) as document_description,
    max(case when (institution.level = 3 OR institution.id IS NULL) then 'HS' else 'UNI' end) as document_institution,
    max(document.created_at) as created_at,
    max(document.published_at) as published_at,
    max(datediff('DAY', document.published_at, current_date)) AS days_since_published,
    max(document.deleted_at) as deleted_at,
    max(case when document.published_at is null and document.deleted_at is null and document.finished = 1 and document.user_accepted = 1
                then 'PENDING_ANALYSIS'
         when document.published_at is null and document.deleted_at is null and not (document.finished = 1 and document.user_accepted = 1)
                then 'PENDING_SUBMISSION'
         when document.published_at is null and document.deleted_at is not null 
                then 'REJECTED'
         when document.published_at is not null and document.deleted_at is not null 
                then 'DELETED'
         when document.published_at is not null and not (institution.active is true AND institution.deleted is false AND course.active is true AND course.deleted_at is null)
                then 'PUBLISHED_NOT_VISIBLE'
         when document.published_at is not null and (institution.active is true AND institution.deleted is false AND course.active is true AND course.deleted_at is null)
                then 'PUBLISHED'
         else 'OTHER'
        end ) as document_status,
    bool_or(not document.no_index) as is_indexed,
    bool_or(document.finished) as is_finished,
    bool_or(document.auto_disapproved) as is_auto_disapproved,
    max(delete_reason.reason) as delete_reason,
    bool_or(document.is_premium) as is_premium,
    max(document.premium_variants) as premium_variants,
    max(document.rating) as rating,
    max(document.rating_positive) as rating_positive,
    max(case when document.rating > 0 then (1.00 * (document.rating_positive)/document.rating) else 0 end) as postive_rating_share,
    max(document.category_id) as category_id,
    max(document_category.category_name) as category_name,
    bool_or(document_file.used_ocr) as document_used_ocr,
    max(document_file.pages) as document_pages,
    max(document_file.extension) as document_extension,
    max(document_file.filesize) as document_filesize,
    max(document_file.white_pixel_count) as document_white_pixel_count,
    bool_or(document_file.detected_slides) as document_detected_slides,
    max(document_file.word_count) as document_word_count,
    max(document_file.average_word_length) as document_average_word_length,
    bool_or(document.anonymous) as is_anonymous,
    max(document.user_id) as uploader_user_id,
    max(user_data.country_id) as uploader_country_id,
    max(user_data.institution_id) as uploader_institution_id,
    bool_or(case when user_blacklist.user_id is not null then 1 else 0 end) as is_user_blacklist,
    bool_or(case when user_profile.is_verified_student = 1 then 1 else 0 end) as is_verified_student,
    bool_or(case when user_profile.has_requested_poe_email = 1 then 1 else 0 end) as has_requested_poe_email, 
    max(document.course_id) as course_id,
    max(course.name) as course_name,
    max(course.code) as course_code,
    bool_or(course.active) as course_active,
    max(course.deleted_at) as course_deleted_at,
    max(institution.id) AS institution_id,
    max(institution.name) AS institution_name,
    max(institution.name_short) AS institution_name_short,
    max(institution.region_code) AS institution_region_code,
    max(institution.phase) AS institution_phase,
    bool_or(institution.active) AS institution_active,
    bool_or(institution.deleted) AS institution_deleted,
    max(institution.level) AS institution_level,
    max(institution.population) AS institution_population,
    max(institution.paid_acquisition::FLOAT) AS institution_acquisition_factor,
    max(country.id) AS country_id,
    max(country.name) AS country_name,
    max(country.geocode) geocode, 
    max(country_tier.tier) AS country_tier,
    max(COALESCE(degree.region_code,institution.region_code)) as document_region_code,
    max(region.high_school_paid_acquisition) as region_acquisition_factor,
    max(degree.id) AS degree_id,
    max(degree.name) as degree_name,
    max(degree.region_code) as degree_region_code,
    max(degree.high_school_paid_acquisition) as degree_acquisition_factor,
    bool_or(degree.is_active) as degree_is_active,
    max(degree.phase) as degree_phase,
    max(grade.id) AS grade_id,
    max(grade.name) AS grade_name,
    bool_or(grade.is_active) AS grade_is_active,
    max(case when document.document_acquisition_type_id = 1 then 'ORGANIC' 
             when document.document_acquisition_type_id = 2 then 'PAID'
             else 'OTHER' end) as document_acquisition_type,
    max(acquisition_batch.id) as acquisition_batch_id,
    max(acquisition_batch.created_at) as acquisition_batch_created_at,
    bool_or(acquisition_batch.action) as acquisition_batch_action,
    max(acquisition_batch.total_offer_euro) as acquisition_batch_total_offer_euro,
    bool_or(acquisition_batch.voided) as acquisition_batch_voided,
    max(acquisition_batch.factor) as acquisition_batch_factor,
    max(acquisition_batch.user_agent) as acquisition_batch_user_agent,
    bool_or(acquisition_batch.is_super_uploader) as acquisition_batch_is_super_uploader,
    max(acquisition_batch_group.id) as acquisition_batch_group_id,
    max(acquisition_batch_group.created_at) as acquisition_batch_group_created_at,
    max(acquisition_batch_group.amount_eur) as acquisition_batch_group_amount_eur,
    bool_or(acquisition_batch_group.paid) as acquisition_batch_group_paid,
    max(payouts_doc.payout_per_doc) as payout_per_doc

FROM 
    "production"."production_studocu"."document" as document
LEFT JOIN 
    "production"."production_studocu"."user" as user_data on user_data.id = document.user_id
LEFT JOIN 
    "production"."production_studocu"."course" as course ON course.id = document.course_id
LEFT JOIN 
    "raw_data"."general"."document_category" ON document_category.category_id = document.category_id
LEFT JOIN 
    "production"."production_studocu"."delete_reason" as delete_reason ON delete_reason.id = document.delete_reason_id
LEFT JOIN
    "production"."production_studocu"."grade" as grade ON grade.id = course.grade_id
LEFT JOIN
    "production"."production_studocu"."degree" as degree ON degree.id = COALESCE(course.degree_id,grade.degree_id)
LEFT JOIN 
    "production"."production_studocu"."institution" as institution ON institution.id = COALESCE(document.institution_id, course.institution_id)
LEFT JOIN 
    "production"."production_studocu"."country" as country ON country.id = institution.country_id
LEFT JOIN 
    "raw_data"."general"."country_tier" ON country.id = country_tier.country_id
LEFT JOIN 
    payouts_doc ON payouts_doc.document_id = document.id
LEFT JOIN 
    "production"."production_studocu"."document_acquisition_batch" as document_acquisition_batch ON document.id = document_acquisition_batch.document_id
LEFT JOIN
    "production"."production_studocu"."acquisition_batch" as acquisition_batch ON document_acquisition_batch.acquisition_batch_id = acquisition_batch.id
LEFT JOIN 
    "production"."production_studocu"."acquisition_batch_group" as acquisition_batch_group ON acquisition_batch.acquisition_batch_group_id = acquisition_batch_group.id
LEFT JOIN
    "production"."production_studocu"."user_blacklist" as user_blacklist ON document.user_id = user_blacklist.user_id
LEFT JOIN
   "production"."production_studocu"."user_profile" as user_profile ON document.user_id = user_profile.user_id 
LEFT JOIN
    "production"."production_studocu"."document_file" as document_file ON document.id = document_file.document_id
LEFT JOIN 
    "production"."production_studocu"."region" as region on institution.region_code = region.code
GROUP BY 1
  );
  