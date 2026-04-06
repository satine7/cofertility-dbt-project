-- The Bridge: links session_id to user_id using events as the join table.
-- This is necessary because sessions and conversions share no direct key.

with events_with_users as (
    select distinct
        session_id,
        user_id
    from {{ ref('stg_events') }}
    where user_id is not null
),

mapped as (
    select
        e.session_id,
        e.user_id,
        s.session_id is not null as has_session_record,
        case
            when s.session_id is not null then s.channel_group
            else 'unattributable'
        end as channel_group,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.landing_page,
        s.session_start
    from events_with_users e
    left join {{ ref('stg_sessions') }} s
        on e.session_id = s.session_id
)

select * from mapped
