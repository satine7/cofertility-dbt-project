-- Pivot events into one row per session with funnel step timestamps and flags.

with events as (
    select * from {{ ref('stg_events') }}
),

aggregated as (
    select
        session_id,
        max(member_type) as member_type,

        -- Timestamps for each funnel step
        min(case when event_type = 'page_viewed' then event_timestamp end) as first_page_view_at,
        min(case when event_type = 'cta_clicked' then event_timestamp end) as cta_clicked_at,
        min(case when event_type = 'prescreener_started' then event_timestamp end) as prescreener_started_at,
        min(case when event_type = 'prescreener_completed' then event_timestamp end) as prescreener_completed_at,
        min(case when event_type = 'account_created' then event_timestamp end) as account_created_at,
        min(case when event_type = 'signup_completed' then event_timestamp end) as signup_completed_at,

        -- Boolean flags for fast filtering
        max(case when event_type = 'page_viewed' then 1 else 0 end)::boolean as has_page_view,
        max(case when event_type = 'cta_clicked' then 1 else 0 end)::boolean as has_cta_click,
        max(case when event_type = 'prescreener_started' then 1 else 0 end)::boolean as has_prescreener_started,
        max(case when event_type = 'prescreener_completed' then 1 else 0 end)::boolean as has_prescreener_completed,
        max(case when event_type = 'account_created' then 1 else 0 end)::boolean as has_account_created,
        max(case when event_type = 'signup_completed' then 1 else 0 end)::boolean as has_signup_completed,

        -- Prescreener outcome
        max(case when event_type = 'prescreener_completed' then prescreener_passed end) as prescreener_passed,

        -- User ID (take the non-null value from any event in the session)
        max(user_id) as user_id,

        count(*) as event_count

    from events
    group by session_id
)

select * from aggregated
