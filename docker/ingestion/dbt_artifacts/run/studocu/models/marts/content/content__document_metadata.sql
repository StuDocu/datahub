
  
    

  create  table
    "production"."content"."document_metadata__dbt_tmp"
    
    
    
  as (
    SELECT
    *
FROM
    "production"."intermediate"."int_content__document_metadata"
  );
  