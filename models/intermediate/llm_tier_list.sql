{{
    config(
        materialized='view'
    )
}}

with base as (
    select * from {{ ref('stg_llm_performance') }}
),

calculate_percentiles as (
    select
        percentile_cont(0.33) within group (order by intelligence_index) as intelligence_33rd,
        percentile_cont(0.67) within group (order by intelligence_index) as intelligence_67th,
        percentile_cont(0.33) within group (order by price_per_1m_tokens) as price_33rd,
        percentile_cont(0.67) within group (order by price_per_1m_tokens) as price_67th
    from base
    where intelligence_index is not null
),

add_tiers as (
    select
        -- Explicitly select cleaned columns from base
        b.model_name,
        b.creator,
        b.context_window_tokens,
        b.intelligence_index,
        b.price_per_1m_tokens,
        b.speed_tokens_per_sec,
        b.latency_seconds,
        b.intelligence_price_ratio,
        b.performance_score,
        b.responsiveness_score,
        
        -- Intelligence Tiers
        case 
            when b.intelligence_index >= p.intelligence_67th then 'High'
            when b.intelligence_index >= p.intelligence_33rd then 'Medium'
            else 'Low'
        end as intelligence_tier,
        
        -- Price Tiers
        case 
            when b.price_per_1m_tokens >= p.price_67th then 'Premium'
            when b.price_per_1m_tokens >= p.price_33rd then 'Standard'
            else 'Budget'
        end as price_tier,
        
        -- Value Category (Sweet Spot Analysis)
        case 
            when b.intelligence_index >= p.intelligence_67th 
                and b.price_per_1m_tokens < p.price_67th then 'Sweet Spot'
            when b.intelligence_index >= p.intelligence_67th 
                and b.price_per_1m_tokens >= p.price_67th then 'Premium Performance'
            when b.intelligence_index < p.intelligence_33rd 
                and b.price_per_1m_tokens < p.price_33rd then 'Budget'
            else 'Standard'
        end as value_category,
        
        -- Speed Tier
        case 
            when b.speed_tokens_per_sec >= 100 then 'Fast'
            when b.speed_tokens_per_sec >= 50 then 'Medium'
            else 'Slow'
        end as speed_tier,
        
        -- Value Rank (based on intelligence-to-price ratio)
        row_number() over (order by b.intelligence_price_ratio desc nulls last) as value_rank

    from base b
    cross join calculate_percentiles p
)

select * from add_tiers