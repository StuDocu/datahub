
  
    

  create  table
    "production"."intermediate"."int_product__institution_popular_courses__dbt_tmp"
    
    
    
  as (
    WITH l30d_document_views AS (
    SELECT
        document_id,
        COUNT(*) as l30d_views
    FROM
        "production"."stg_mixpanel"."document_view" document_view
    INNER JOIN
        "production"."production_studocu"."document" document
        ON document_view.document_id = document.id
    INNER JOIN
        "production"."production_studocu"."course" course
        ON document.course_id = course.id
    WHERE
        DATE(event_datetime) >= DATEADD(day, -30, GETDATE())
        AND document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND document.no_index = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
    GROUP BY    
        1
),
course_documents AS (
    SELECT
        document.institution_id,
        document.course_id,
        COALESCE(SUM(l30d_views), 0) as l30d_views,
        SUM(CASE WHEN document.published_at::date >= date_add('month', -15, current_date)::date THEN 1 ELSE 0 END) as number_of_documents
    FROM
        "production"."production_studocu"."document" document
    INNER JOIN 
        "production"."production_studocu"."course" course
        ON document.course_id = course.id
    LEFT JOIN
        l30d_document_views
        ON document.id = l30d_document_views.document_id
    WHERE
        document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND document.no_index = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
        AND course.user_count IS NOT NULL
    GROUP BY
        1,
        2
),
institution_course_rank AS (
    SELECT
        institution_id,
        course_id,
        ROW_NUMBER() OVER (PARTITION BY institution_id ORDER BY l30d_views DESC, number_of_documents DESC) AS course_rank
    FROM
        course_documents
    WHERE
        number_of_documents > 0
)
SELECT
    'popular' as "type",
    'course' as model_type,
    course_id as model_id,
    institution_id
FROM
    institution_course_rank
WHERE   
    course_rank <= 200
ORDER BY
    4,
    3
  );
  