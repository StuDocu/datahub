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
consolidated_doc_downloads AS (
    SELECT
        DATE(doc_download.event_datetime) as date,
        doc_download.document_id:: INT AS document_id,
        COUNT( doc_download.distinct_id ) AS total_doc_downloads,
        COUNT( DISTINCT doc_download.distinct_id ) AS doc_downloads_unique_users,
        COUNT( DISTINCT CASE WHEN doc_download.mp_country_code = c.geocode THEN doc_download.distinct_id END) AS doc_downloads_unique_local_users,
        COUNT( CASE WHEN doc_download.mp_country_code = c.geocode THEN doc_download.distinct_id END) AS total_doc_downloads_local
    FROM
        "production"."stg_mixpanel"."document_download" as doc_download
    LEFT JOIN
        "production"."production_studocu"."country"  as c on c.id = university_country_id
    INNER JOIN 
        valid_docs ON valid_docs.id = doc_download.document_id:: INT
    

    WHERE
        date = DATE('2025-04-01')
 

group by 1,2
)
SELECT
    *
FROM
    consolidated_doc_downloads
ORDER BY
    1,
    2