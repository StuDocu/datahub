WITH consolidated_doc_reports AS (
    SELECT
        DATE(doc_report.created_at) as date,
        doc_report.document_id:: INT AS document_id,
        COUNT( doc_report.user_id ) AS total_doc_reports,
        COUNT( distinct doc_report.user_id ) AS unique_reporters
    FROM
        "production"."production_studocu"."report" as doc_report
    INNER JOIN 
        "production"."production_studocu"."document" as doc ON doc.id = doc_report.document_id
    WHERE 
        doc.published_at is not null and 
        doc.deleted_at is null

          

    and 
        date = DATE('2025-02-06')
 
group by 1,2

)
SELECT
    *
FROM
    consolidated_doc_reports
ORDER BY
    1,
    2