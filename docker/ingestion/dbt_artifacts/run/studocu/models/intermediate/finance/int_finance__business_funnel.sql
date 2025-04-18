
  
    

  create  table
    "production"."intermediate"."int_finance__business_funnel__dbt_tmp"
    
    
    
  as (
    with 
    user_funnel as (
        select * 
        from "production"."intermediate"."int_finance__user_funnel"  
    ), 
    business_funnel as (
        select 
            dt
            , country_id
            , country_name
            , country_tier 
            , institution_type
            -- views
            , count(distinct distinct_id) as viewing_users
            , sum(views_total) as total_views
            , sum(views_paywall) as paywall_views
            , count(distinct case when views_paywall > 0 then distinct_id end) as users_with_paywall_view
            -- payment started
            , count(distinct case when has_payment_started = 1 then distinct_id end) as users_with_payment_started
            -- payment attempts
            , sum(coalesce(payment_attempts_before_subscription,0)) as payment_attempts_before_subscription
            -- authorised payments
            , count(distinct subscription_id) as subscriptions 
            , count(distinct case when billing_cycle = 3 then subscription_id end) as quarterly_subscriptions 
            , count(distinct case when billing_cycle = 12 then subscription_id end) as annual_subscriptions 
            , count(distinct case when has_subscription = 1 then user_id end) as users_with_subscription
            , count(distinct case when is_trial = 1 then subscription_id end) as trials
            -- full payments
            , count(distinct case when has_full_payment = 1 then subscription_id end) as subscriptions_with_full_payment
            , count(distinct case when has_full_payment = 1 then user_id end) as users_with_full_payment
        from user_funnel 
        group by 1, 2, 3, 4, 5
        order by 1, 2, 3, 4, 5
    )

select * 
from business_funnel
  );
  