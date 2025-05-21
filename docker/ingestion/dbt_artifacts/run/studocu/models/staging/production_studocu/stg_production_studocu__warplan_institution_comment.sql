
  
    

  create  table
    "production"."production_studocu"."warplan_institution_comment__dbt_tmp"
    
    diststyle key distkey (institution_id)
    
  as (
    
WITH source AS (
    SELECT 
        DISTINCT
        cs.subject_id AS institution_id,
        c.created_at,
        CASE WHEN c.comment LIKE 'UB_%' THEN 'UB'
            WHEN c.comment LIKE 'EB_%' THEN 'EB'
            WHEN c.comment LIKE 'UC_%' THEN 'UC'
        END AS comment_subtype,
        CASE WHEN c.comment LIKE 'UB_%' THEN replace(c.comment,'UB_','')
            WHEN c.comment LIKE 'EB_%' THEN replace(c.comment,'EB_','')
            WHEN c.comment LIKE 'UC_%' THEN replace(c.comment,'UC_','')
        END AS comment,
        c.deleted_at
    FROM "production"."production_studocu"."comment" AS c
    LEFT JOIN "production"."production_studocu"."comment_subject" AS cs ON cs.comment_id = c.id AND cs.subject_type = 'Institution'
     WHERE c.comment LIKE 'UB_%' OR c.comment LIKE 'EB_%' OR c.comment LIKE 'UC_%'
),
converted AS (
    SELECT
        "institution_id" :: INTEGER AS "institution_id",
        "created_at" :: TIMESTAMP AS "created_at",
        "comment_subtype" ::VARCHAR AS "comment_subtype",
        "comment" ::VARCHAR AS "comment",
        "deleted_at" :: TIMESTAMP AS "deleted_at"
    FROM
        source
)
SELECT
    *
FROM
    converted
  );
  