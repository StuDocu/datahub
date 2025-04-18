with raw_uni_logs as  -- adding derived parameters for cleaning 
(
SELECT
    logs.institution_id AS institution_id,
    logs.created_at as stage_change_valid_from,
    logs.current_stage AS from_stage,
    logs.new_stage AS to_stage,
    lag(logs.current_stage) over(partition by logs.institution_id order by logs.created_at asc) as prev_from_stage, 
    lag(logs.new_stage) over(partition by logs.institution_id order by logs.created_at asc) as prev_to_stage,
    lag(logs.current_stage,2) over(partition by logs.institution_id order by logs.created_at asc) as prev_prev_from_stage, 
    lag(logs.new_stage,2) over(partition by logs.institution_id order by logs.created_at asc) as prev_prev_to_stage,
    lead(logs.current_stage) over(partition by logs.institution_id order by logs.created_at asc) as next_from_stage, 
    lead(logs.new_stage) over(partition by logs.institution_id order by logs.created_at asc) as next_to_stage,
    DATEDIFF(day, logs.created_at, coalesce(lead(logs.created_at) over(partition by logs.institution_id order by logs.created_at asc),getdate())) as days_in_logged_stage,
    DATEDIFF(day, lag(logs.created_at) over(partition by logs.institution_id order by logs.created_at asc),logs.created_at) as prev_days_in_logged_stage
FROM
    "production"."metrics_studocu"."institution_stage_log" as logs
WHERE 
    true and 
    from_stage != to_stage  -- stage changed from and to should not be the same
and logs.created_at != '0002-11-30 BC'
ORDER BY 
    2,3 asc, 1 desc
), 

cleaning_to_fro_logs as -- remove to_fro changes 
(
    with mark_to_fro_record as -- mark cyclic stage changes (Eg: 1-> 2 -> 1) within 10 days (threshold)
    (
        SELECT 
            *, 
            case when days_in_logged_stage <= 10  and  
                    prev_to_stage = next_to_stage then 1
                when days_in_logged_stage <= 10  and     
                    prev_to_stage is null and 
                    from_stage = next_to_stage then 1
                else 0 end as flag_to_fro
        FROM 
           raw_uni_logs
    ),
    clean_to_fro_record as   -- remove cyclic stage changes
    (
        SELECT 
            * 
        FROM 
            mark_to_fro_record
        WHERE flag_to_fro = 0
    )
    
    SELECT                    -- recomputing derived parameters for further cleaning 
        institution_id, 
        stage_change_valid_from,
        from_stage, 
        to_stage,
        lead(from_stage) over(partition by institution_id order by stage_change_valid_from asc) as next_from_stage, 
        lead(to_stage) over(partition by institution_id order by stage_change_valid_from asc) as next_to_stage,
        lag(from_stage) over(partition by institution_id order by stage_change_valid_from asc) as prev_from_stage, 
        lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc) as prev_to_stage,
        DATEDIFF(day, stage_change_valid_from, coalesce(lead(stage_change_valid_from) over(partition by institution_id order by stage_change_valid_from asc ),getdate())) as days_in_logged_stage,
        DATEDIFF(day, lag(stage_change_valid_from) over(partition by institution_id order by stage_change_valid_from asc),stage_change_valid_from) as prev_days_in_logged_stage,
        flag_to_fro
    FROM 
        clean_to_fro_record
),

cleaning_other_error_logs as 
(
    with mark_other_error_logs as 
    (

        SELECT 
            *, 
            case when 
                prev_days_in_logged_stage <= 10 and    -- Mark to combine step by step updates (1->2->3 = 1->3) within 10 days (threshold)
                to_stage != prev_from_stage and 
                from_stage = prev_to_stage then 1 
                else 0 end as flag_combine,
            case when 
                days_in_logged_stage <= 10 and          -- Mark the final to stage to combine
                to_stage = next_from_stage and 
                from_stage != next_to_stage then next_to_stage
                end as new_next_to_stage,  
            case when                                       -- Mark duplicate updates on consecutive days (1->2, 1->2 = 1->2) within 10 days (threshold)
                to_stage = prev_to_stage and 
                from_stage = prev_from_stage then 1 
                else 0 end as flag_duplicate,               -- Mark multiple updates on the same days to retain only the latest update of the day 
            case when 
                prev_days_in_logged_stage = 0 then 1 
                else 0 end as flag_mutiple_update
        FROM 
            cleaning_to_fro_logs
    ),
    
    clean_other_error_logs as              -- remove marked records and recomputing derived parameters for further cleaning 
    (
    
    SELECT
        institution_id,
        stage_change_valid_from,
        from_stage, 
        case when new_next_to_stage is not null then new_next_to_stage 
             else to_stage end as to_stage, -- updated new "to_stage" for step by step stage update logs combined into one stage log (Eg: 1->2->3 = 1->3)
        DATEDIFF(day, stage_change_valid_from, coalesce(lead(stage_change_valid_from) over(partition by institution_id order by stage_change_valid_from asc ),getdate())) as days_in_logged_stage
    FROM 
        mark_other_error_logs 
    WHERE 
        flag_duplicate = 0 and 
        flag_mutiple_update = 0 and 
        flag_combine = 0
    ORDER BY 
        2,3 asc, 1 desc
    ), 
    
    check_stage_sync as     -- check the stage sync among the stage logs (previous to_stage should be same as current from_stage )
    (
    SELECT 
        *,
        case when (lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc) is null) then 0
             when (from_stage = lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc)) then 0
             when (to_stage = lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc)) then 1 
             else 0 end as flag_delete,  -- Sync records where there is a mismatch due to previous cleaning (`Eg: 1->2, 3->2 = 1->2 )   
        case when (lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc) is null) then 0
             when (from_stage = lag(to_stage) over(partition by institution_id order by stage_change_valid_from asc)) then 0 
             else 1 end as flag_sync -- Mark non-sync records
    FROM 
        clean_other_error_logs 
    ORDER BY 
        2, 3, 1 desc
    
    )
    
    SELECT    -- Clean records to fix stage sync because of cleaning. The flag is retained to check if there is any sync issue from the raw data that were not cleaned before (external error).
        *, 
        case when DATEDIFF(day,stage_change_valid_from, max(stage_change_valid_from) over(partition by institution_id )) = 0 then 1 
             else 0 end as is_recent_stage_change_log 
    FROM 
        check_stage_sync 
    WHERE flag_delete = 0
), 

uni_age as( -- Earliest document date as the university creation date
SELECT 
    doc.institution_id, 
    DATE(MIN(doc.published_at)) as first_document_published_at
FROM 
    "production"."production_studocu"."document" as doc   
WHERE 
    doc.institution_id IN ( SELECT distinct cleaning_other_error_logs.institution_id FROM cleaning_other_error_logs)
    AND doc.published_at IS NOT NULL 
GROUP BY 
    1
)

SELECT
    logs.institution_id,
    trunc(stage_change_valid_from) as valid_from_date,
    trunc(coalesce(lead(stage_change_valid_from) over(partition by logs.institution_id order by stage_change_valid_from asc ),getdate())) as valid_until_date,
    from_stage,
    to_stage, 
    CASE WHEN to_stage > from_stage  
        THEN 'Upgraded'  
        ELSE 'Downgraded' 
    END as stage_change_type,
    DATEDIFF(day, trunc(stage_change_valid_from), trunc(coalesce(lead(stage_change_valid_from) over(partition by logs.institution_id order by stage_change_valid_from asc ),getdate()))) as days_in_logged_stage,
    is_recent_stage_change_log, 
    institution.phase as current_stage,
    DATEDIFF(day, first_document_published_at, stage_change_valid_from ) as institution_age_as_on_log_date,
    case when
        is_recent_stage_change_log = 1 and 
        institution.phase != to_stage then 1
        else flag_sync end as is_sync_error
FROM 
    cleaning_other_error_logs as logs
LEFT JOIN 
    "production"."production_studocu"."institution" as institution on institution.id =  logs.institution_id
LEFT JOIN 
    uni_age ON uni_age.institution_id = logs.institution_id
ORDER BY 
    1, 2