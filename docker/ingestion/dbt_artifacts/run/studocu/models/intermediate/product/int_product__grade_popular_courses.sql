
  
    

  create  table
    "production"."intermediate"."int_product__grade_popular_courses__dbt_tmp"
    
    
    
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
        AND course.user_count IS NOT NULL
    GROUP BY    
        1
),
grade_documents as (
    select 
        course.grade_id,
        document.course_id as course_id,
        course.user_count as user_count,
        COALESCE(SUM(l30d_views), 0) as l30d_views,
        SUM(CASE WHEN document.published_at::date >= date_add('month', -15, current_date)::date THEN 1 ELSE 0 END) as number_of_documents
    from
        "production"."production_studocu"."document"
    inner join 
        "production"."production_studocu"."course"
        on document.course_id = course.id
    left join
        l30d_document_views
        ON document.id = l30d_document_views.document_id
    where
        document.published_at is not null
        and document.deleted_at is null
        and course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and course.degree_id is null -- to avoid picking up documents in courses specific to one particular degree
        and document.no_index = 0
        and course.active = 1
        and course.deleted_at is null
        AND course.user_count is not null
    group by
        1,
        2,
        3
),
grade_course_rank AS (
    SELECT
        grade_id,
        course_id,
        ROW_NUMBER() OVER (PARTITION BY grade_id ORDER BY user_count DESC, l30d_views DESC, number_of_documents DESC) AS course_rank
    FROM
        grade_documents
    WHERE
        number_of_documents > 0
)
SELECT
    'popular' as "type",
    'course' as model_type,
    course_id as model_id,
    grade_id
FROM
    grade_course_rank
WHERE   
    course_rank <= 20
ORDER BY
    4,
    3
  );
  