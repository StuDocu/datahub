

  create view "production"."intermediate"."int_content__organic_uploads__dbt_tmp" as (
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
organic_uploads AS (
    SELECT 
        CAST(COALESCE(d.published_at, d.created_at) as date) as published_at,
        CASE
            WHEN d.user_id = 0 THEN 0
            ELSE 1 END
            as verified_user,
        'missing' as utm_source,
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
                WHEN d.user_accepted=1 AND d.finished=1 THEN 1
                ELSE 0 END
        ) AS inbound_documents,
        SUM(
            CASE
                WHEN d.user_accepted=1 AND d.finished=1 AND d.published_at IS NULL AND d.deleted_at IS NOT NULL THEN 1
                ELSE 0 END
        ) as studocu_rejected_documents,
        SUM(
            CASE
                WHEN d.user_accepted=0 AND d.finished=1 AND d.published_at IS NULL THEN 1
                ELSE 0 END
        ) as user_rejected_documents,
        SUM(
            CASE
                WHEN d.user_accepted=1 AND d.finished=1 AND d.published_at IS NOT NULL AND d.deleted_at IS NULL THEN 1
                ELSE 0 END
        ) as approved_documents
    FROM
        "production"."production_studocu"."document" d
        LEFT JOIN "production"."production_studocu"."course" cr ON d.course_id = cr.id
        LEFT JOIN "production"."production_studocu"."user" u ON d.user_id = u.id
        LEFT JOIN "production"."production_studocu"."user_profile" up ON u.id = up.user_id
        LEFT JOIN "production"."production_studocu"."institution" i ON d.institution_id = i.id
        LEFT JOIN "production"."production_studocu"."country" ct ON i.country_id = ct.id
        LEFT JOIN "raw_data"."general"."country_tier" ctt ON ct.id = ctt.country_id
        LEFT JOIN "raw_data"."general"."document_category" dc ON d.category_id = dc.category_id
        LEFT JOIN "raw_data"."general"."document_acquisition" da ON d.document_acquisition_type_id = da.document_acquisition_type_id
    WHERE
        da.document_acquisition_type = 'organic'
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
        13
),
institution_stage AS (
    SELECT
        d.date,
        usl.id as institution_id,
        usl.stage as institution_stage
    FROM 
        dates d
        LEFT JOIN "production"."intermediate"."int_universities_stage_logs" usl ON d.date = usl.date
)

SELECT 
    published_at,
    verified_user,
    utm_source,
    country_id,
    country_name,
    geocode,
    country_tier,
    ou.institution_id,
    institution_name,
    institution_stage,
    course_id,
    course_name,
    category_name,
    document_acquisition_type,
    number_of_uploaded_documents,
    inbound_documents,
    studocu_rejected_documents,
    user_rejected_documents,
    approved_documents
FROM organic_uploads ou
LEFT JOIN institution_stage inst
ON ou.institution_id = inst.institution_id AND ou.published_at = inst.date
  ) with no schema binding;
