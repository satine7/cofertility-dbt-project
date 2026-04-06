with source as (
    select * from {{ ref('raw_conversions') }}
),

cleaned as (
    select
        user_id,
        cast(converted_at as timestamp) as converted_at,
        lower(plan_type) as plan_type
    from source
),

-- Deduplicate: keep earliest conversion per user
-- (handles retry duplicates for usr_136 and usr_143)
deduplicated as (
    select
        user_id,
        converted_at,
        plan_type,
        row_number() over (partition by user_id order by converted_at) as rn
    from cleaned
)

select
    user_id,
    converted_at,
    plan_type
from deduplicated
where rn = 1
