
  
    

  create  table
    "production"."content"."institutions_content_milestones__dbt_tmp"
    
    
    
  as (
    SELECT
    *    
FROM
    "production"."intermediate"."int_content__institutions_content_milestones"
ORDER BY 1
  );
  