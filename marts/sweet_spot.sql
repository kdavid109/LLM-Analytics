{{
    config(
        materialized='table'
    )
}}

-- Summary table answering: Which are the "sweet spot" models?
-- And: Which models offer the best intelligence-to-price ratio?
with base as (
    select * from {{ ref('llm_tier_list') }}
),

top_value_models as (
    select
        model_name,
        creator,
        intelligence_index,
        price_per_1m_tokens,
        speed_tokens_per_sec,
        latency_seconds,
        intelligence_price_ratio as value_score,
        intelligence_tier,
        price_tier,
        value_category,
        value_rank,
        
        -- Create recommendation reason
        case 
            when value_category = 'Sweet Spot' then 'High intelligence at reasonable price'
            when value_category = 'Premium Performance' then 'Highest intelligence available'
            when value_category = 'Budget' then 'Most affordable option'
            else 'Balanced performance and price'
        end as recommendation_reason,
        
        -- Overall score (weighted combination)
        round(
            (intelligence_index * 0.4) + 
            ((1 / nullif(price_per_1m_tokens, 0)) * 1000 * 0.3) + 
            (speed_tokens_per_sec * 0.2) + 
            ((1 / nullif(latency_seconds, 0)) * 10 * 0.1),
            2
        ) as overall_score

    from base
    where intelligence_index is not null
),

flag_best_models as (
    select
        *,
        case 
            when value_rank <= 10 then true
            else false
        end as is_top_10_value,
        
        case 
            when value_category = 'Sweet Spot' then true
            else false
        end as is_sweet_spot,
        
        case 
            when overall_score >= percentile_cont(0.75) within group (order by overall_score) 
                over () then true
            else false
        end as is_top_quartile

    from top_value_models
)

select * from flag_best_models
order by value_score desc