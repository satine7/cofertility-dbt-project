-- First-touch attribution spine.
-- For each user, identifies the first session they appeared in
-- and its channel attribution.

with ranked as (
    select
        user_id,
        session_id,
        channel_group,
        utm_source,
        utm_campaign,
        landing_page,
        coalesce(session_start, (
            select min(event_timestamp)
            from {{ ref('stg_events') }} e
            where e.session_id = m.session_id
        )) as effective_session_start,
        row_number() over (
            partition by user_id
            order by coalesce(session_start, '2099-01-01'::timestamp)
        ) as rn
    from {{ ref('int_session_user_mapping') }} m
)

select
    user_id,
    session_id as first_touch_session_id,
    channel_group as first_touch_channel,
    utm_source as first_touch_utm_source,
    utm_campaign as first_touch_utm_campaign,
    landing_page as first_touch_landing_page,
    effective_session_start as first_touch_at
from ranked
where rn = 1
