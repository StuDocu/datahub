
      insert into "production"."mixpanel"."int_mixpanel__daily_document_downloads_per_document" ("date", "document_id", "total_doc_downloads", "doc_downloads_unique_users", "doc_downloads_unique_local_users", "total_doc_downloads_local")
    (
        select "date", "document_id", "total_doc_downloads", "doc_downloads_unique_users", "doc_downloads_unique_local_users", "total_doc_downloads_local"
        from "int_mixpanel__daily_document_downloads_per_document__dbt_tmp031140894505"
    )


  