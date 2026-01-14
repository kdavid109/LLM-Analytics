{{
    config(
        materialized='table'
    )
}}

-- Main fact table for model-level analysis in Power BI
with base as (
    select * from {{ ref('llm_tier_list') }}
),

add_rankings as (
    select
        *,
        -- Rankings for various metrics
        row_number() over (order by intelligence_index desc) as intelligence_rank,
        row_number() over (order by price_per_1m_tokens asc) as price_rank_asc,
        row_number() over (order by speed_tokens_per_sec desc) as speed_rank,
        row_number() over (order by latency_seconds asc) as latency_rank,
        
        -- Percentile rankings
        percent_rank() over (order by intelligence_price_ratio) as value_percentile,
        percent_rank() over (order by intelligence_index) as intelligence_percentile,
        percent_rank() over (order by price_per_1m_tokens desc) as affordability_percentile

    from base
)

select * from add_rankings