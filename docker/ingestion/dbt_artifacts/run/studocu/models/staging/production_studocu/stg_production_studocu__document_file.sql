
  
    

  create  table
    "production"."production_studocu"."document_file__dbt_tmp"
    
    
    
  as (
    WITH base AS (
    SELECT
        
    CONVERT_TIMEZONE(
        'Europe/Amsterdam',
        'Europe/Amsterdam',
        TIMESTAMP 'epoch' + timestamp :: bigint * INTERVAL '1 second'
    )
 AS "created_at",
        id,
        "timestamp",
        document_id,
        coalesce("name", '') as "name",
        object_key,
        extension,
        original_extension,
        thumbnail_sizes,
        "version",
        html5,
        in_uploads,
        in_source,
        in_assets,
        in_text,
        used_ocr,
        active,
        pages,
        filesize,
        white_pixel_count,
        detected_slides,
        uploader_type,
        simhash,
        processed,
        markdown_version,
        word_count,
        pixel_count,
        average_word_length,
        coalesce(summary, '') as summary,
        upload_source
    FROM
        
  

  (
    select *
    from "production"."datalake_production_studocu"."document_file"
    
  )

)
SELECT
    *,
    
    
md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(document_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(object_key as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(extension as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(original_extension as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(thumbnail_sizes as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(version as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(html5 as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(in_uploads as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(in_source as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(in_assets as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(in_text as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(used_ocr as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(active as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pages as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(filesize as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(white_pixel_count as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(detected_slides as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(uploader_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(simhash as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(processed as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(markdown_version as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(word_count as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pixel_count as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(average_word_length as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(summary as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(upload_source as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS etl_md5
FROM
    base
  );
  