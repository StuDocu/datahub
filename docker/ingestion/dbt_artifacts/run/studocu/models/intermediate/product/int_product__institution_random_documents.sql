
  
    

  create  table
    "production"."intermediate"."int_product__institution_random_documents__dbt_tmp"
    
    
    
  as (
    WITH random_documents AS (
    SELECT
        document.id as document_id,
        document.institution_id as institution_id,
        ROW_NUMBER() OVER (PARTITION BY document.institution_id ORDER BY RANDOM()) as document_rank
    FROM
        "production"."production_studocu"."document" as document
    INNER JOIN
        "production"."production_studocu"."course" as course
        ON document.course_id = course.id
    WHERE
        document.published_at IS NOT NULL
        AND document.deleted_at IS NULL
        AND document.no_index = 0
        AND course.active = 1
        AND course.deleted_at IS NULL
)
SELECT
    'random' as "type",
    'document' as model_type,
    document_id as model_id,
    institution_id
FROM
    random_documents
WHERE
    document_rank <= 900
ORDER BY
    4,
    3
  );
  