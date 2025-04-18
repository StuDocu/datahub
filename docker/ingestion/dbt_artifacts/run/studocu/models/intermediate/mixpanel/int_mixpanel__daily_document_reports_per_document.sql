
      insert into "production"."mixpanel"."int_mixpanel__daily_document_reports_per_document" ("date", "document_id", "total_doc_reports", "unique_reporters")
    (
        select "date", "document_id", "total_doc_reports", "unique_reporters"
        from "int_mixpanel__daily_document_reports_per_document__dbt_tmp031140933188"
    )


  