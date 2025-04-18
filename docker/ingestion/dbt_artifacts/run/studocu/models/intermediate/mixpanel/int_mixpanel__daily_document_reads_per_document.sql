
      insert into "production"."mixpanel"."int_mixpanel__daily_document_reads_per_document" ("date", "document_id", "total_doc_reads", "doc_reads_unique_users", "doc_reads_unique_local_users", "total_doc_reads_local")
    (
        select "date", "document_id", "total_doc_reads", "doc_reads_unique_users", "doc_reads_unique_local_users", "total_doc_reads_local"
        from "int_mixpanel__daily_document_reads_per_document__dbt_tmp031140977456"
    )


  