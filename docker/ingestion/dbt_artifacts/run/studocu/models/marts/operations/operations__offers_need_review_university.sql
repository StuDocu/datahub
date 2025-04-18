
  
    

  create  table
    "production"."operations"."offers_need_review_university__dbt_tmp"
    
    
    
  as (
    
with  __dbt__cte__operations__offers_need_review_base as (


with source as (
    with user_data as (
        select
            u.id as user_id,
            c.name as user_country_name,
            u.deleted
        from "production"."production_studocu"."user" as u
        left join "production"."production_studocu"."country" as c
            on u.country_id = c.id
        group by 1, 2, 3
    ), user_emails as (
        select
            user_id,
            email as user_email
        from "production"."production_studocu"."user_email"
        group by 1, 2
    ), user_paypal_emails as (
        select
            user_id,
            email as paypal_email
        from "production"."production_studocu"."acquisition_batch"
        group by 1, 2
    ), user_student_blacklisted_emails as (
    	SELECT
    	    use.user_id,
    	    sum((ub.user_id is not null)::int) > 0 as is_blacklisted
    	from user_emails as use
        left join "production"."production_studocu"."user_blacklist" as ub
    		    on use.user_email = ub.email
    	group by 1
    ), user_paypal_blacklisted_emails as (
    	SELECT
    	    upe.user_id,
    	    sum((ub.user_id is not null)::int) > 0 as is_blacklisted
    	from user_paypal_emails as upe
        left join "production"."production_studocu"."user_blacklist" as ub
    		    on upe.paypal_email = ub.email
    	group by 1
    ), user_payout as (
        select
            user_id,
            SUM(amount_eur) AS user_total_paid_amount
        from "production"."production_studocu"."payout"
        where status = 'SUCCESS'
        group by 1
    ), user_account_verification_email_data as (
        select
            user_id,
            email,
            row_number() OVER (PARTITION BY user_id ORDER BY id desc) as rnk
        from "production"."production_studocu"."user_email"
        where
            for_student_verification = true
            and deleted = false
            and activated = true
    ), user_account_verification_email_count as ( -- pick all emails
        select
            user_id,
            count(email) as student_verification_emails_count
        from "production"."production_studocu"."user_email"
        where
            for_student_verification = true
            and deleted = false
            and activated = true
        group by 1
    ), user_account_verification_email as ( -- pick last of all the verified emails
        select
            user_id,
            email as student_verification_email
        from user_account_verification_email_data
        where rnk = 1
        group by 1, 2
    ), current_offers_raw as (
        select
            ab.user_id,
            ab.id as batch_id,
            ab.total_offer_euro,
            count(distinct d.id) as offer_docs_count
        FROM "production"."production_studocu"."acquisition_batch" as ab
        join "production"."production_studocu"."document_acquisition_batch" as dab
            on ab.id = dab.acquisition_batch_id
        join "production"."production_studocu"."document" as d
            on d.id = dab.document_id
        where
            exists (
                select 1
                from "production"."production_studocu"."user_profile" as up
                where ab.user_id = up.user_id)
            and exists (
                select 1
                from "production"."production_studocu"."institution" as i
                where
                    i.id = ab.institution_id)
            and ab.paid = False
            and ab.voided = False
            and ab."action" = True
            and dab.finished_at is not NULL
            and d.auto_disapproved = false
            and d.deleted_at is null
        group by 1, 2, 3
    ), current_offers as (
        select
            user_id,
            sum(total_offer_euro) as total_offer_euro,
            sum(offer_docs_count) as total_offer_docs_count,
            sum(total_offer_euro)/sum(offer_docs_count) as price_per_doc
        from current_offers_raw
        group by 1
    ), historical_offer_institution_count as (
        SELECT
            ab.user_id,
            count(distinct ab.institution_id) as historical_offer_institution_count
        FROM "production"."production_studocu"."acquisition_batch" as ab
        join "production"."production_studocu"."document_acquisition_batch" as dab
            on ab.id = dab.acquisition_batch_id
        join "production"."production_studocu"."document" as d
            on d.id = dab.document_id
        where
            dab.finished_at is not NULL
            and d.document_acquisition_type_id = 2
        group by 1
    ), offers_data as (
        SELECT
            ab.user_id,
            sum(
                case
                    when
                        ab.paid = true
                        and d.document_acquisition_type_id = 2
                        and d.published_at is not NULL
                        and d.deleted_at is NULL
                        and not d.auto_disapproved = True
                            then 1
                else 0
            end) as published_paid_documents_count
        FROM "production"."production_studocu"."acquisition_batch" as ab
        join "production"."production_studocu"."document_acquisition_batch" as dab
            on ab.id = dab.acquisition_batch_id
        join "production"."production_studocu"."document" as d
            on d.id = dab.document_id
        group by 1
    ), documents_data as (
        select
            user_id,
            count(id) as infringing_documents_count
        from "production"."production_studocu"."document"
        where deleted_at is not null and delete_reason_id = 5
        group by 1
    ), user_email_duplicated_accounts as (
        select
            ue1.user_id as user_id,
            ue2.user_id as duplicate_user_id
        from "production"."production_studocu"."user_email" as ue1
        join "production"."production_studocu"."user_email" as ue2
            ON (ue1.email = ue2.email
            AND ue1.user_id != ue2.user_id)
        group by 1, 2
    ), user_paypal_email_duplicated_accounts as (
        select
            ab1.user_id as user_id,
            ab2.user_id as duplicate_user_id
        from "production"."production_studocu"."acquisition_batch" as ab1
        join "production"."production_studocu"."acquisition_batch" as ab2
            ON (ab1.email = ab2.email
            AND ab1.user_id != ab2.user_id)
        group by 1, 2
    ), user_duplicated_accounts as (
        select
            user_id,
            duplicate_user_id
        from user_email_duplicated_accounts

        union

        select
            user_id,
            duplicate_user_id
        from user_paypal_email_duplicated_accounts
    ), user_duplicated_accounts_count as (
        select
            user_id,
            count(distinct duplicate_user_id) as secondary_accounts_count
        from user_duplicated_accounts
        group by 1
    ), user_duplicated_accounts_data as (
        select
            uda.user_id,
            listagg(
                COALESCE(
                    u.first_name || ' ' || u.last_name,
                    u.first_name,
                    u.last_name),
                ', '
            ) as secondary_accounts_names,
            listagg(
                distinct duplicate_user_id, ', '
            ) as secondary_accounts_ids
        from user_duplicated_accounts as uda
        join "production"."production_studocu"."user" as u
            on u.id = uda.duplicate_user_id
        group by 1
    ), user_email_counts as (
        select
            user_id,
            count(distinct duplicate_user_id) as user_email_linked_account_count
        from user_email_duplicated_accounts
        group by 1
    ), paypal_email_counts as (
        select
            user_id,
            count(distinct duplicate_user_id) as paypal_email_linked_account_count
        from user_paypal_email_duplicated_accounts
        group by 1
--    ), user_country_data as (
--        select
--            ab.user_id,
--            ci3.name as country,
--            uoi.institution_country_id != ci3.name as is_different_ip_offer_country
--        from
--            "production"."production_studocu"."acquisition_batch" as ab
--        join "production"."general".ip_country_preset as icp
--            on ab.ip_address = icp.ip_address
--        join "production"."general".country_iso_3166 as ci3
--            on ci3."alpha-2" = icp.country
--        left join user_offer_institution as uoi
--            on uoi.user_id = ab.user_id
--        group by 1, 2, 3
    ), blacklisted_users as (
        select
            ud.user_id,
            ub.id is not NULL as is_blacklisted
        from user_data as ud
        left join "production"."production_studocu"."user_blacklist" as ub
           on ud.user_id = ub.user_id
        group by 1, 2
    ), user_ip_usage as (
            SELECT
              ab1.user_id,
              COUNT(DISTINCT ab2.user_id) AS ip_usage_count
            FROM
              "production"."production_studocu"."acquisition_batch" ab1
            JOIN
              "production"."production_studocu"."acquisition_batch" ab2 ON ab1.ip_address = ab2.ip_address
            GROUP BY
        ab1.user_id
    )
    select
        u.user_id,
        u.deleted as is_user_deleted,
        
    
        CAST(NULL AS TEXT)
    
 as student_verification_email,
        uavec.student_verification_emails_count,
        bu.is_blacklisted,
        upbe.is_blacklisted as is_paypal_email_used_by_blocklisted,
        usbe.is_blacklisted as is_user_email_is_used_by_blocklisted,
        pec.paypal_email_linked_account_count,
        listagg(distinct upeda.duplicate_user_id, ', ') as paypal_email_linked_accounts_ids,
        sec.user_email_linked_account_count,
        listagg(distinct useda.duplicate_user_id, ', ') as user_email_linked_account_ids,
        hoic.historical_offer_institution_count,
        udac.secondary_accounts_count,
        udad.secondary_accounts_names,
        udad.secondary_accounts_ids,
        up.user_total_paid_amount,
        dd.infringing_documents_count,
        co.total_offer_euro,
        co.total_offer_docs_count,
        co.price_per_doc,
        uiu.ip_usage_count,
        od.published_paid_documents_count
    from user_data as u
    left join user_payout as up
        on up.user_id = u.user_id
    left join documents_data as dd
        on dd.user_id = u.user_id
    left join offers_data as od
        on od.user_id = u.user_id
    left join blacklisted_users as bu
        on bu.user_id = u.user_id
    left join user_emails as use
        on use.user_id = u.user_id
    left join user_paypal_emails as upe
        on upe.user_id = u.user_id
    left join user_duplicated_accounts as uda
        on uda.user_id = u.user_id
    left join user_email_counts as sec
        on sec.user_id = u.user_id
    left join paypal_email_counts as pec
        on pec.user_id = u.user_id
    left join user_duplicated_accounts_count as udac
        on udac.user_id = u.user_id
    left join user_duplicated_accounts_data as udad
        on udad.user_id = u.user_id
    left join user_account_verification_email as uave
        on uave.user_id = u.user_id
    left join user_account_verification_email_count as uavec
        on uavec.user_id = u.user_id
    left join user_student_blacklisted_emails as usbe
        on usbe.user_id = u.user_id
    left join user_paypal_blacklisted_emails as upbe
        on upbe.user_id = u.user_id
    left join historical_offer_institution_count as hoic
        on hoic.user_id = u.user_id
    left join user_email_duplicated_accounts as useda
        on useda.user_id = u.user_id
    left join user_paypal_email_duplicated_accounts as upeda
        on upeda.user_id = u.user_id
    left join current_offers as co
        on co.user_id = u.user_id
    left join user_ip_usage as uiu
        on uiu.user_id = u.user_id
    group by
        1, 2, 3, 4, 5, 6, 7, 8,
        10, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22
)
select * from source
), eligible_acquisition_batch as (
        select
            ab.id,
            ab.user_id,
            ab.cookie_paypal_address,
            ab.institution_id,
            ab.ip_address,
            ab.created_at,
            ab.email
        from
            "production"."production_studocu"."acquisition_batch" as ab
        where
            exists (
                select 1
                from "production"."production_studocu"."user_profile" as up
                where ab.user_id = up.user_id
                    and is_verified_student = True)
            and exists (
                select 1
                from "production"."production_studocu"."institution" as i
                where
                    i.id = ab.institution_id
                    and i.level != 3)
            and exists (
                select 1
                from "production"."production_studocu"."document_acquisition_batch" as dab
                where
                    ab.id = dab.acquisition_batch_id
                    and dab.finished = True)
            and ab.paid = False
            and ab.voided = False
            and ab.acquisition_batch_group_id is null
            and ab.action = True
            and ab.updated_at <= getdate() - interval '6 hours'
            and ab.institution_id is not null
            and ab.user_id != 0  -- test account generates a lot of batches
        group by 1, 2, 3, 4, 5, 6, 7
    ), eligible_users as (
        select
            user_id
        from
            eligible_acquisition_batch
        group by 1
    ), country_purchase_open_preset as (
        select
            country_id,
            max(paid_acquisition) > 0 as is_country_open_to_purchase
        from "production"."production_studocu"."institution"
        group by 1
    ), user_offer_institution as (
        select
            ab.user_id,
            ab.institution_id,
            u.country_id as user_country_id,
            i.country_id as institution_country_id,
            c.name as institution_country_name,
            i.name as institution_name,
            max(cpop.is_country_open_to_purchase::int)::bool as is_country_open_to_purchase,
            u.country_id != i.country_id as is_different_user_offer_country
        from eligible_acquisition_batch as ab
        join "production"."production_studocu"."user" as u
            on ab.user_id = u.id
        join "production"."production_studocu"."institution" as i
            on ab.institution_id = i.id
        join "production"."production_studocu"."country" as c
            on c.id = i.country_id
        join country_purchase_open_preset as cpop
            on cpop.country_id = i.country_id
        join "production"."production_studocu"."document_acquisition_batch" as dab
            on ab.id = dab.acquisition_batch_id
        join "production"."production_studocu"."document" as d
            on d.id = dab.document_id
        where
            d.document_acquisition_type_id = 2
            and d.user_accepted = True
        group by 1, 2, 3, 4, 5, 6
    ), acquisition_batch_data as (
        SELECT
            user_id,
            listagg(distinct id, ', ') as batch_ids,
            listagg(distinct email, ', ') as offer_emails,
            listagg(distinct ip_address, ', ') as users_ip_addresses,
            sum((GETDATE() - interval '90 days' < created_at)::int) as offers_last_90_days_count,
            max(created_at) as latest_acquisition_batch_finished_at
        FROM eligible_acquisition_batch
        group by 1
    ), institution_count as (
        select
            user_id,
            count(distinct(institution_id)) as institutions_of_upload_count
        from user_offer_institution
        group by 1
    ), users_invites_data as (
        select
            user_id,
            listagg(distinct referrering_user_id, ', ')  as referrers_and_referries
        from (
        select
            invitee_user_id as user_id,
            inviter_user_id as referrering_user_id
        from
            "production"."production_studocu"."invitation"

        union

        select
            inviter_user_id as user_id,
            invitee_user_id as referrering_user_id
        from
            "production"."production_studocu"."invitation"
        )
        group by 1
    ), user_country_data as (
        select
            ab.user_id,
            ci3.name as country,
            uoi.institution_country_name != ci3.name as is_different_ip_offer_country
        from
            eligible_acquisition_batch as ab
        join "production"."general".ip_country_preset as icp
            on ab.ip_address = icp.ip_address
        join "production"."general".country_iso_3166 as ci3
            on ci3."alpha-2" = icp.country
        left join user_offer_institution as uoi
            on uoi.user_id = ab.user_id
        group by 1, 2, 3
    )
    select
      u.*,
      abd.batch_ids,
      
    
        CAST(NULL AS TEXT)
    
 as offer_emails,
      
    
        CAST(NULL AS TEXT)
    
 as users_ip_addresses,
      abd.latest_acquisition_batch_finished_at,
      listagg(distinct uoi.institution_name, ', ') as offer_institution_names,
      listagg(distinct uoi.institution_country_name, ', ') as offer_institution_countries,
      listagg(distinct ucd.country, ', ') as ip_country, -- could be multiple countries for different offers
      max(uoi.is_country_open_to_purchase::int)::bool as is_country_open_to_purchase,
      case
          when max(ucd.is_different_ip_offer_country::int) > 0
              then True
          else False
      end as is_different_ip_offer_country,
      case
            when max(uoi.is_different_user_offer_country::int) > 0
                then True
            else False
        end as is_user_offer_institution_different,
      ic.institutions_of_upload_count,
      abd.offers_last_90_days_count,
      uid.referrers_and_referries,
      GETDATE() as datamart_updated_at
    from __dbt__cte__operations__offers_need_review_base as u
    left join acquisition_batch_data as abd
        on abd.user_id = u.user_id
    left join user_offer_institution as uoi
        on uoi.user_id = u.user_id
    left join institution_count as ic
        on ic.user_id = u.user_id
    left join user_country_data as ucd
        on ucd.user_id = u.user_id
    left join users_invites_data as uid
        on uid.user_id = u.user_id
    where
        exists (
            select 1
            from eligible_users as eu
            where u.user_id = eu.user_id)
    group by
        1, 2, 3, 4, 5, 6, 7, 8, 9,
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22,
        -- grouped fields above come from 'operations__offers_need_review_base'
        23, 24, 25, 26,
        33, 34, 35
  );
  