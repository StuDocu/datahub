

WITH
-- Keep only the columns we need
clean_metrics AS (
    SELECT
        wv.event_datetime,
        wv.ttfb_value,
        wv.ttfb_rating,
        wv.lcp_value,
        wv.lcp_rating,
        wv.lcp_time_to_first_byte,
        wv.lcp_resource_load_delay,
        wv.lcp_element_render_delay,
        wv.inp_value,
        wv.inp_rating,
        wv.cls_value,
        wv.cls_rating,
        CASE
            WHEN wv.page_name = 'DocumentViewer' THEN 'Document'
            ELSE wv.page_name
            END AS page_group,
        wv.mp_current_url AS url,
        wv.user_id,
        wv.resolved_distinct_id,
        wv.user_signed_in,
        wv.user_language,
        wv.user_country_id,
        wv.user_country_name,
        wv.mp_search_engine,
        wv.mp_initial_referrer,
        wv.mp_os,
        wv.mp_browser,
        CASE
            WHEN wv.mp_screen_width < 768 THEN 'Mobile'
            WHEN wv.mp_screen_width < 1200 THEN 'Tablet'
            WHEN wv.mp_screen_width >= 1200 THEN 'Desktop'
            ELSE 'Unknown'
        END AS device_type,
        wv.hash_distinct_id,
        wv.connection_type
    FROM "production"."mixpanel"."web_vital_metric" AS wv
    WHERE TRUE
        AND wv.ttfb_value IS NOT NULL
        AND wv.lcp_value IS NOT NULL
        AND wv.inp_value IS NOT NULL
        AND wv.cls_value IS NOT NULL
        
        AND wv.event_datetime::DATE = date('2025-04-01')
        
),
-- Enhance previous CTE with important segments
enhanced_metrics AS (
    SELECT
        cm.*,
        -- Extract id depending on the page group
        CASE
            WHEN page_group NOT IN ('Question', 'Course Category', 'Course Questions', 'Course Quizzes') 
                AND SPLIT_PART(REGEXP_REPLACE(url, '^.*/', ''), '?', 1) ~ '^[0-9]+$' -- Extract last number as ID
                THEN SPLIT_PART(REGEXP_REPLACE(url, '^.*/', ''), '?', 1)
            WHEN page_group = 'Question'  AND SPLIT_PART(url, '/', 7) ~ '^[0-9]+$' -- Special case for Question page
                THEN SPLIT_PART(url, '/', 7)
            WHEN page_group IN ('Course Category', 'Course Questions', 'Course Quizzes') 
                AND REVERSE(SPLIT_PART(REVERSE(url), '/', 2)) ~ '^[0-9]+$' -- Special case for course category & questions
                THEN REVERSE(SPLIT_PART(REVERSE(url), '/', 2))
            ELSE NULL
            END AS id,
        SPLIT_PART(url, '/', 4) AS url_region,
        ct.country_id AS url_country_id,
        -- Define row and latam as separate country entities
        CASE
            WHEN url_region IN ('latam', 'row') THEN url_region
            ELSE ct.country_name 
            END AS url_country_name,
        COALESCE(ct.country_tier::VARCHAR, 'no_tier') AS url_country_tier
    FROM clean_metrics AS cm
    LEFT JOIN "production"."production_studocu"."country_region" AS cr ON SPLIT_PART(cm.url, '/', 4) = cr.region_code AND SPLIT_PART(cm.url, '/', 4) NOT IN ('latam', 'row')
    LEFT JOIN "raw_data"."general"."country_tiers_full" AS ct ON cr.country_id = ct.country_id
    
),
-- Enhance data with institution ids and types in separate CTEs, to avoid too complex logic in one CTE
institutions_metrics AS (
    SELECT
        em.*,
        -- Extract institution_id based on page type
        COALESCE(i_doc.id, i_course.id, i_inst.id) AS institution_id,
        CASE
            WHEN COALESCE(i_doc.level, i_course.level, i_inst.level) = 3 
                OR page_group IN ('Degree', 'HighSchoolDegreeOverview') THEN 'high_school'
            WHEN COALESCE(i_doc.level, i_course.level, i_inst.level) ~ '^[0-9]+$'  THEN 'university' -- Only numeric values
            ELSE 'unknown' END AS institution_type
    FROM enhanced_metrics AS em
    -- Sequence of joins to get instutution ids and types based on page types
    LEFT JOIN "production"."production_studocu"."document" AS d ON (CASE WHEN em.page_group = 'Document' THEN em.id END) = d.id 
    LEFT JOIN "production"."production_studocu"."course" AS c ON (CASE WHEN em.page_group = 'Course' THEN em.id END) = c.id
    LEFT JOIN "production"."production_studocu"."institution" AS i_doc ON d.institution_id = i_doc.id -- Join institution for documents
    LEFT JOIN "production"."production_studocu"."institution" AS i_course ON c.institution_id = i_course.id -- Join institution for courses
    LEFT JOIN "production"."production_studocu"."institution" AS i_inst ON (CASE WHEN em.page_group = 'Institution' THEN em.id END) = i_inst.id -- Join institution for institutions
)

SELECT 
    *
FROM 
    institutions_metrics