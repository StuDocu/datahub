
  
    

  create  table
    "production"."intermediate"."int_product__degree_popular_courses__dbt_tmp"
    
    
    
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
degree_documents as (
    select 
        coalesce(course.degree_id, degree.id) as degree_id,
        document.course_id as course_id,
        course.user_count as user_count,
        COALESCE(SUM(l30d_views), 0) as l30d_views,
        COUNT(document.id) as number_of_documents
    from
        "production"."production_studocu"."document"
    inner join 
        "production"."production_studocu"."course"
        on document.course_id = course.id
    left join
        "production"."production_studocu"."grade"
        on course.grade_id = grade.id
    left join
        "production"."production_studocu"."degree"
        on grade.degree_id = degree.id
    left join
        l30d_document_views
        ON document.id = l30d_document_views.document_id
    where
        document.published_at is not null
        and document.deleted_at is null
        and course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and document.no_index = 0
        and coalesce(course.degree_id, degree.id) is not null
        and course.active = 1
        and course.deleted_at is null
        AND course.user_count is not null
    group by
        1,
        2,
        3
),
degree_course_rank AS (
    SELECT
        degree_id,
        course_id,
        ROW_NUMBER() OVER (PARTITION BY degree_id ORDER BY user_count DESC, l30d_views DESC, number_of_documents DESC) AS course_rank
    FROM
        degree_documents
    WHERE
        number_of_documents > 0
)
SELECT
    'popular' as "type",
    'course' as model_type,
    course_id as model_id,
    degree_id
FROM
    degree_course_rank
WHERE   
    course_rank <= 20
ORDER BY
    4,
    3
  );
  