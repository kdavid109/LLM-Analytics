{{
    config(
        materialized='table'
    )
}}

-- Summary table answering: What's the average cost by intelligence tier?
-- And: What's the price premium for higher intelligence scores?
with base as (
    select * from {{ ref('llm_tier_list') }}
),

tier_analysis as (
    select
        intelligence_tier,
        
        -- Counts
        count(*) as model_count,
        
        -- Price metrics
        round(avg(price_per_1m_tokens), 2) as avg_price,
        round(min(price_per_1m_tokens), 2) as min_price,
        round(max(price_per_1m_tokens), 2) as max_price,
        round(percentile_cont(0.5) within group (order by price_per_1m_tokens), 2) as median_price,
        round(stddev(price_per_1m_tokens), 2) as price_stddev,
        
        -- Intelligence metrics
        round(avg(intelligence_index), 2) as avg_intelligence,
        round(min(intelligence_index), 2) as min_intelligence,
        round(max(intelligence_index), 2) as max_intelligence,
        
        -- Performance metrics
        round(avg(speed_tokens_per_sec), 2) as avg_speed,
        round(avg(latency_seconds), 3) as avg_latency,
        round(avg(intelligence_price_ratio), 2) as avg_value_score

    from base
    where intelligence_index is not null
    group by intelligence_tier
),

calculate_premiums as (
    select
        *,
        -- Calculate price premium vs Low tier
        round(
            avg_price - lag(avg_price) over (order by 
                case intelligence_tier 
                    when 'Low' then 1 
                    when 'Medium' then 2 
                    when 'High' then 3 
                end
            ), 
            2
        ) as price_premium_vs_lower_tier,
        
        -- Premium as percentage
        round(
            ((avg_price - lag(avg_price) over (order by 
                case intelligence_tier 
                    when 'Low' then 1 
                    when 'Medium' then 2 
                    when 'High' then 3 
                end
            )) / nullif(lag(avg_price) over (order by 
                case intelligence_tier 
                    when 'Low' then 1 
                    when 'Medium' then 2 
                    when 'High' then 3 
                end
            ), 0)) * 100,
            2
        ) as price_premium_pct

    from tier_analysis
)

select * from calculate_premiums
order by case intelligence_tier 
    when 'Low' then 1 
    when 'Medium' then 2 
    when 'High' then 3 
end