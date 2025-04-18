
  
    

  create  table
    "production"."intermediate"."int_product__grade_recent_documents__dbt_tmp"
    
    
    
  as (
    WITH document_recency_rank AS (
    select 
        document.id as document_id,
        course.grade_id,
        ROW_NUMBER() OVER (PARTITION BY course.grade_id ORDER BY document.published_at DESC) as document_rank
    from
        "production"."production_studocu"."document"
    inner join 
        "production"."production_studocu"."course"
        on document.course_id = course.id
    where
        document.published_at is not null
        and document.deleted_at is null
        and course.institution_id is null -- to avoid picking up documents in courses specific to one particular high school
        and course.grade_id is null
        and document.no_index = 0
        and course.active = 1
        and course.deleted_at is null
)
SELECT
    'recent' as "type",
    'document' as model_type,
    document_id as model_id,
    grade_id
FROM
    document_recency_rank
WHERE   
    document_rank <= 200
ORDER BY
    4,
    3
  );
  