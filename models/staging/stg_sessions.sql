with source as (
    select * from {{ ref('raw_sessions') }}
),

cleaned as (
    select
        session_id,
        lower(nullif(utm_source, '')) as utm_source,
        lower(nullif(utm_medium, '')) as utm_medium,
        lower(replace(nullif(utm_campaign, ''), '-', '_')) as utm_campaign,
        landing_page,
        cast(session_start as timestamp) as session_start,

        -- Derived channel group
        case
            when nullif(utm_medium, '') = 'cpc' then 'paid_' || lower(nullif(utm_source, ''))
            when nullif(utm_medium, '') = 'email' then 'email'
            when nullif(utm_medium, '') = 'referral' then 'referral'
            else 'direct_or_organic'
        end as channel_group,

        -- Flag for missing UTMs
        case when nullif(utm_source, '') is null then true else false end as is_direct

    from source
)

select * from cleaned
