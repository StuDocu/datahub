

  create view "production"."intermediate"."int_content__paid_uploads__dbt_tmp" as (
    WITH dates AS (
    SELECT
        "date"
    FROM
        "production"."general"."calendar"
    WHERE
        "date" >= CAST(
            '2016-01-01' AS DATE
        )
),
paid_uploads AS (
    SELECT 
        CAST(COALESCE(d.published_at, d.created_at) as date) as published_at,
        ab.id as acquisition_batch_id,
        d.user_id,
        CASE d.user_id
            WHEN 0 THEN 0
            ELSE 1 END 
            as verified_user, 
        ct.id as country_id,
        ct.name as country_name,
        ct.geocode,
        ctt.tier as country_tier,
        i.id as institution_id,
        i.name as institution_name,
        cr.id as course_id,
        cr.name as course_name,
        dc.category_name,
        da.document_acquisition_type,
        COUNT(d.id) as number_of_uploaded_documents,
        SUM(
            CASE
                WHEN d.finished=1 AND dab.finished IS NOT NULL AND d.user_accepted=1 AND ab.action = 1 AND (up.is_verified_student = 1 OR up.has_requested_poe_email = 1) THEN 1
                ELSE 0 END
        ) as inbound_documents,
        SUM(
            CASE
                WHEN d.finished=1 AND dab.finished IS NOT NULL AND d.user_accepted=1 AND ab.action = 1 AND d.published_at IS NULL AND d.deleted_at IS NOT NULL AND (up.is_verified_student = 1 OR up.has_requested_poe_email = 1) THEN 1
                ELSE 0 END
        ) as studocu_rejected_documents,
        SUM(
            CASE
                WHEN d.finished=1 AND dab.finished IS NOT NULL AND d.user_accepted=0 AND ab.action = 1 AND d.published_at IS NULL AND (up.is_verified_student = 1 OR up.has_requested_poe_email = 1) THEN 1
                ELSE 0 END
        ) as user_rejected_documents,
        SUM(
            CASE
                WHEN d.finished=1 AND dab.finished IS NOT NULL AND d.user_accepted=1 AND ab.action = 1 AND d.published_at IS NOT NULL AND d.deleted_at IS NULL AND (up.is_verified_student = 1 or up.has_requested_poe_email = 1) THEN 1
                ELSE 0 END
        ) as approved_documents
    FROM
        "production"."production_studocu"."document_acquisition_batch" dab
        LEFT JOIN "production"."production_studocu"."acquisition_batch" ab on ab.id = dab.acquisition_batch_id
        LEFT JOIN "production"."production_studocu"."document" d ON dab.document_id = d.id
        LEFT JOIN "production"."production_studocu"."course" cr ON d.course_id = cr.id
        LEFT JOIN "production"."production_studocu"."user" u ON d.user_id = u.id
        LEFT JOIN "production"."production_studocu"."user_profile" up ON u.id = up.user_id
        LEFT JOIN "production"."production_studocu"."institution" i ON d.institution_id = i.id
        LEFT JOIN "production"."production_studocu"."country" ct ON i.country_id = ct.id
        LEFT JOIN "raw_data"."general"."country_tier" ctt ON ct.id = ctt.country_id
        LEFT JOIN "raw_data"."general"."document_category" dc ON d.category_id = dc.category_id
        LEFT JOIN "raw_data"."general"."document_acquisition" da ON d.document_acquisition_type_id = da.document_acquisition_type_id
    WHERE
        da.document_acquisition_type = 'paid'
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14
),
institution_stage AS (
    SELECT
        d.date,
        usl.id as institution_id,
        usl.stage as institution_stage
    FROM 
        dates d
        LEFT JOIN "production"."intermediate"."int_universities_stage_logs" usl ON d.date = usl.date
),
-- Retrieve the source information of the first upload event of a user on a given day and take this as the source of all completed uploads on that day
-- In about 9% of cases, the Nth upload source is different from the first (https://data.studocu.com/question/2147-first-vs-all-paid-acquisition-sources)
first_started_paid_upload_event AS (
    SELECT DISTINCT
        CAST(event_datetime AS date) as created_at,
        COALESCE(CAST(user_id as varchar), distinct_id) as user_id,
        CASE 
            WHEN user_id IS NULL THEN 0
            ELSE 1 END
            as verified_user,
        FIRST_VALUE(utm_source) 
            OVER(PARTITION BY COALESCE(CAST(user_id as varchar), distinct_id), CAST(event_datetime as date) 
                 ORDER BY event_datetime
                 rows between unbounded preceding and unbounded following) as utm_source
    FROM
        "production"."stg_mixpanel"."document_acquisition_started"
),
first_started_paid_upload_source AS (
    SELECT
        created_at,
        user_id,
        verified_user,
        CASE 
            WHEN utm_source LIKE '%facebook%' OR utm_source LIKE '%fb ' THEN 'facebook'
            WHEN utm_source LIKE '%google%adwords%' THEN 'google adwords'
            WHEN utm_source LIKE '%customerioleadads%' THEN 'e-mail leads'  -- This is evaluated hierarchically, so it does not conflict with the next line
            WHEN utm_source LIKE '%customerio%' OR utm_source LIKE '%e%mails%' THEN 'e-mail'
            WHEN utm_source LIKE '%instagram%' THEN 'instagram'
            WHEN utm_source LIKE '%youtube%' OR utm_source = 'YT' THEN 'youtube'
            WHEN utm_source LIKE '%tiktok%' THEN 'tiktok'
            WHEN utm_source IS NULL THEN 'missing'
            ELSE 'other' END
        as utm_source
    FROM first_started_paid_upload_event 
),
completed_uploads AS (
    SELECT
        CAST(event_datetime as date) as created_at,
        distinct_id as user_id,
        CASE
            WHEN REGEXP_COUNT(distinct_id, '^[0-9]+$') = 1 THEN 1 --Only fully numeric user_ids (i.e. distinct_ids) are registered users
            ELSE 0 END 
            as verified_user,
        offer_id as acquisition_batch_id
    FROM
        "production"."stg_mixpanel"."document_acquisition_offer_complete"
    GROUP BY 1,
             2,
             3,
             4 -- To avoid duplicates
),
completed_uploads_with_source AS (
    SELECT 
        fspus.created_at,
        fspus.user_id,
        COALESCE(cu.verified_user, fspus.verified_user) as verified_user,
        COALESCE(fspus.utm_source, 'missing') as utm_source,
        cu.acquisition_batch_id
    FROM
        first_started_paid_upload_source fspus
        LEFT JOIN completed_uploads cu 
        ON fspus.created_at = cu.created_at AND fspus.user_id = cu.user_id
),
paid_uploads_with_source AS (
    SELECT 
        pu.published_at,
        pu.acquisition_batch_id,
        pu.user_id,
        COALESCE(pu.verified_user, cuws.verified_user) as verified_user,
        COALESCE(cuws.utm_source, 'missing') as utm_source,
        pu.country_id,
        pu.country_name,
        pu.geocode,
        pu.country_tier,
        pu.institution_id,
        pu.institution_name,
        inst.institution_stage,
        pu.course_id,
        pu.course_name,
        pu.category_name,
        pu.document_acquisition_type,
        pu.number_of_uploaded_documents,
        pu.inbound_documents,
        pu.studocu_rejected_documents,
        pu.user_rejected_documents,
        pu.approved_documents
    FROM
        paid_uploads pu
        LEFT JOIN completed_uploads_with_source cuws 
        ON pu.acquisition_batch_id = cuws.acquisition_batch_id
        LEFT JOIN institution_stage inst
        ON pu.institution_id = inst.institution_id AND pu.published_at = inst.date
)
SELECT 
    published_at,
    verified_user,
    utm_source,
    country_id,
    country_name,
    geocode,
    country_tier,
    institution_id,
    institution_name,
    institution_stage,
    course_id,
    course_name,
    category_name,
    document_acquisition_type,
    SUM(number_of_uploaded_documents) as number_of_uploaded_documents,
    SUM(inbound_documents) as inbound_documents,
    SUM(studocu_rejected_documents) as studocu_rejected_documents,
    SUM(user_rejected_documents) as user_rejected_documents,
    SUM(approved_documents) as approved_documents
FROM paid_uploads_with_source
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14
  ) with no schema binding;
