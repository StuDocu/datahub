
      insert into "production"."intermediate"."int_seo__page_speed_metrics" ("event_datetime", "ttfb_value", "ttfb_rating", "lcp_value", "lcp_rating", "lcp_time_to_first_byte", "lcp_resource_load_delay", "lcp_element_render_delay", "inp_value", "inp_rating", "cls_value", "cls_rating", "page_group", "url", "user_id", "resolved_distinct_id", "user_signed_in", "user_language", "user_country_id", "user_country_name", "mp_search_engine", "mp_initial_referrer", "mp_os", "mp_browser", "device_type", "hash_distinct_id", "connection_type", "id", "url_region", "url_country_id", "url_country_name", "url_country_tier", "institution_id", "institution_type")
    (
        select "event_datetime", "ttfb_value", "ttfb_rating", "lcp_value", "lcp_rating", "lcp_time_to_first_byte", "lcp_resource_load_delay", "lcp_element_render_delay", "inp_value", "inp_rating", "cls_value", "cls_rating", "page_group", "url", "user_id", "resolved_distinct_id", "user_signed_in", "user_language", "user_country_id", "user_country_name", "mp_search_engine", "mp_initial_referrer", "mp_os", "mp_browser", "device_type", "hash_distinct_id", "connection_type", "id", "url_region", "url_country_id", "url_country_name", "url_country_tier", "institution_id", "institution_type"
        from "int_seo__page_speed_metrics__dbt_tmp031156442483"
    )


  