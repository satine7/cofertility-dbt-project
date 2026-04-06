-- Channel performance mart: one row per channel per month.
-- Canonical source for marketing reporting, replaces ad-hoc Metabase queries.

with funnel as (
    select * from {{ ref('fct_session_funnel') }}
),

channel_spend as (
    select
        cast(month as date) as month,
        lower(channel) as channel,
        cast(spend_usd as decimal(10,2)) as spend_usd,
        impressions,
        clicks
    from {{ ref('raw_channel_spend') }}
),

monthly_metrics as (
    select
        date_trunc('month', session_start)::date as month,
        channel_group as channel,

        count(*) as sessions,
        sum(case when has_page_view then 1 else 0 end) as page_views,
        sum(case when has_cta_click then 1 else 0 end) as cta_clicks,
        sum(case when has_prescreener_started then 1 else 0 end) as prescreener_starts,
        sum(case when has_prescreener_completed then 1 else 0 end) as prescreener_completions,
        sum(case when has_account_created then 1 else 0 end) as accounts_created,
        sum(case when has_signup_completed then 1 else 0 end) as signups_completed,
        sum(case when is_converted then 1 else 0 end) as conversions

    from funnel
    group by 1, 2
),

joined as (
    select
        m.month,
        m.channel,
        m.sessions,
        m.page_views,
        m.cta_clicks,
        m.prescreener_starts,
        m.prescreener_completions,
        m.accounts_created,
        m.signups_completed,
        m.conversions,

        coalesce(cs.spend_usd, 0) as spend_usd,
        cs.impressions,
        cs.clicks,

        -- Efficiency metrics
        case when m.conversions > 0
            then round(coalesce(cs.spend_usd, 0) / m.conversions, 2)
            else null
        end as cac_usd,

        case when cs.clicks > 0
            then round(cs.spend_usd / cs.clicks, 2)
            else null
        end as cost_per_click,

        case when m.sessions > 0
            then round(m.conversions * 100.0 / m.sessions, 2)
            else null
        end as conversion_rate_pct,

        -- Funnel drop-off rates
        case when m.page_views > 0
            then round(m.cta_clicks * 100.0 / m.page_views, 2)
            else null
        end as page_to_cta_pct,

        case when m.prescreener_starts > 0
            then round(m.prescreener_completions * 100.0 / m.prescreener_starts, 2)
            else null
        end as prescreener_completion_pct

    from monthly_metrics m
    left join channel_spend cs
        on m.month = cs.month
        and m.channel = 'paid_' || cs.channel
)

select * from joined
order by month, channel
