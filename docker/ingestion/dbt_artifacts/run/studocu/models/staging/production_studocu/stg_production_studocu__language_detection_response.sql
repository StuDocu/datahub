
  
    

  create  table
    "production"."production_studocu"."language_detection_response__dbt_tmp"
    
    diststyle key distkey (document_id)
    
      compound sortkey(created_at)
  as (
    
WITH base AS (
    SELECT
        "id",
        "document_id",
        "py_script_result",
        "fast_text_result",
        "created_at",
        "updated_at"
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."language_detection_response"
    
  )

)
SELECT
    *
FROM
    base
  );
  