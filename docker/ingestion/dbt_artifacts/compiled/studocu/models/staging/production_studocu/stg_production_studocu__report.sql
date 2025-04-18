
WITH BASE AS
  (SELECT "id",
          "document_id",
          "user_id",
          
    
        CAST(NULL AS TEXT)
    
 AS "ip_address",
          "edited_by",
          "created_at",
          "updated_at",
          "reason",
          "accepted",
          "handled",
          "reason_id",
          
    
        CAST(NULL AS TEXT)
    
 AS "extra_data"
   FROM 
  

  (
    select *
    from "production"."datalake_production_studocu"."report"
    
  )
)
SELECT *
FROM BASE