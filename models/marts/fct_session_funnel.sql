-- Primary analytical table: one row per session with full funnel + attribution.
-- Serves both pre-signup (session-level drop-off) and post-signup (conversion) analysis.

with sessions as (
    select * from {{ ref('stg_sessions') }}
),

funnel as (
    select * from {{ ref('int_funnel_events_aggregated') }}
),

user_mapping as (
    select * from {{ ref('int_session_user_mapping') }}
),

first_touch as (
    select * from {{ ref('int_user_first_touch') }}
),

conversions as (
    select * from {{ ref('stg_conversions') }}
),

joined as (
    select
        -- Session identifiers
        coalesce(s.session_id, f.session_id) as session_id,
        f.user_id,
        f.member_type,

        -- Session attribution
        coalesce(s.utm_source, 'unknown') as utm_source,
        coalesce(s.utm_medium, 'unknown') as utm_medium,
        coalesce(s.utm_campaign, 'unknown') as utm_campaign,
        coalesce(s.channel_group, 'unattributable') as channel_group,
        coalesce(s.landing_page, 'unknown') as landing_page,
        coalesce(s.session_start, f.first_page_view_at) as session_start,
        coalesce(s.is_direct, true) as is_direct,

        -- First-touch attribution (user-level)
        ft.first_touch_channel,
        ft.first_touch_utm_source,
        ft.first_touch_utm_campaign,
        ft.first_touch_landing_page,

        -- Funnel step timestamps
        f.first_page_view_at,
        f.cta_clicked_at,
        f.prescreener_started_at,
        f.prescreener_completed_at,
        f.account_created_at,
        f.signup_completed_at,

        -- Funnel step flags
        coalesce(f.has_page_view, false) as has_page_view,
        coalesce(f.has_cta_click, false) as has_cta_click,
        coalesce(f.has_prescreener_started, false) as has_prescreener_started,
        coalesce(f.has_prescreener_completed, false) as has_prescreener_completed,
        coalesce(f.has_account_created, false) as has_account_created,
        coalesce(f.has_signup_completed, false) as has_signup_completed,
        f.prescreener_passed,

        -- Conversion info
        c.converted_at is not null as is_converted,
        c.converted_at,
        c.plan_type,

        -- Derived metrics
        f.event_count,
        case
            when f.has_signup_completed then 'signup_completed'
            when f.has_account_created then 'account_created'
            when f.has_prescreener_completed then 'prescreener_completed'
            when f.has_prescreener_started then 'prescreener_started'
            when f.has_cta_click then 'cta_clicked'
            when f.has_page_view then 'page_viewed'
            else 'no_events'
        end as furthest_funnel_step,

        -- Orphan session flag
        s.session_id is null and f.session_id is not null as is_orphan_session

    from funnel f
    left join sessions s on f.session_id = s.session_id
    left join first_touch ft on f.user_id = ft.user_id
    left join conversions c on f.user_id = c.user_id
)

select * from joined
