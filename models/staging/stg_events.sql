with source as (
    select * from {{ ref('raw_events') }}
),

parsed as (
    select
        event_type,
        nullif(user_id, '') as user_id,
        session_id,
        cast(event_timestamp as timestamp) as event_timestamp,
        properties,

        -- Extract commonly used JSON properties into typed columns
        json_extract_string(properties, '$.call_site') as call_site,
        json_extract_string(properties, '$.member_type') as member_type,
        json_extract_string(properties, '$.page') as page,
        json_extract_string(properties, '$.form') as form,
        lower(json_extract_string(properties, '$.plan')) as plan_type,
        json_extract_string(properties, '$.source') as source_property,
        json_extract_string(properties, '$.campaign') as campaign_property,
        json_extract_string(properties, '$.cta') as cta,
        cast(json_extract_string(properties, '$.passed') as boolean) as prescreener_passed,
        cast(json_extract_string(properties, '$.retry') as boolean) as is_retry

    from source
)

select *
from parsed
where coalesce(is_retry, false) = false
