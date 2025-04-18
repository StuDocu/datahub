WITH valid_docs as 
(
    SELECT 
        id 
    FROM 
        "production"."production_studocu"."document" 
    WHERE 
        published_at is not null and 
        deleted_at is null
),
consolidated_doc_reads AS (
    SELECT
       DATE(doc_read.event_datetime) as date,
        doc_read.document_id:: INT AS document_id,
        COUNT( doc_read.distinct_id ) AS total_doc_reads,
        COUNT( DISTINCT doc_read.distinct_id ) AS doc_reads_unique_users,
        COUNT( DISTINCT CASE WHEN doc_read.mp_country_code = c.geocode THEN doc_read.distinct_id END) AS doc_reads_unique_local_users,
        COUNT( CASE WHEN doc_read.mp_country_code = c.geocode THEN doc_read.distinct_id END) AS total_doc_reads_local
    FROM
        "production"."stg_mixpanel"."document_read" as doc_read
    LEFT JOIN
        "production"."production_studocu"."country"  as c on c.id = university_country_id
    INNER JOIN 
        valid_docs ON  valid_docs.id = doc_read.document_id:: INT

         

    WHERE
        date = DATE('2025-04-01')
 

group by 1,2

)
SELECT
    *
FROM
    consolidated_doc_reads
ORDER BY
    1,
    2