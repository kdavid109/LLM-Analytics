{{
    config(
        materialized='table'
    )
}}

-- Matrix analysis for price vs intelligence visualization
with base as (
    select * from {{ ref('llm_tier_list') }}
),

matrix_summary as (
    select
        intelligence_tier,
        price_tier,
        
        count(*) as model_count,
        round(avg(intelligence_index), 2) as avg_intelligence,
        round(avg(price_per_1m_tokens), 2) as avg_price,
        round(avg(intelligence_price_ratio), 2) as avg_value_score,
        
        -- List models in this quadrant
        listagg(model_name, ', ') within group (order by intelligence_price_ratio desc) as models,
        
        -- Best model in this quadrant (using FIRST_VALUE instead)
        first_value(model_name) over (
            partition by intelligence_tier, price_tier 
            order by intelligence_price_ratio desc nulls last
        ) as best_model

    from base
    where intelligence_index is not null
    group by intelligence_tier, price_tier, model_name, intelligence_price_ratio
),

add_quadrant_labels as (
    select
        *,
        case 
            when intelligence_tier = 'High' and price_tier = 'Budget' then 'Best Value'
            when intelligence_tier = 'High' and price_tier = 'Standard' then 'Sweet Spot'
            when intelligence_tier = 'High' and price_tier = 'Premium' then 'Premium Performance'
            when intelligence_tier = 'Medium' and price_tier = 'Budget' then 'Budget Friendly'
            when intelligence_tier = 'Medium' and price_tier = 'Standard' then 'Balanced'
            when intelligence_tier = 'Medium' and price_tier = 'Premium' then 'Overpriced'
            when intelligence_tier = 'Low' and price_tier = 'Budget' then 'Entry Level'
            when intelligence_tier = 'Low' and price_tier = 'Standard' then 'Poor Value'
            when intelligence_tier = 'Low' and price_tier = 'Premium' then 'Avoid'
            else 'Unknown'
        end as quadrant_label

    from matrix_summary
)

select * from add_quadrant_labels
order by 
    case intelligence_tier when 'High' then 1 when 'Medium' then 2 else 3 end,
    case price_tier when 'Budget' then 1 when 'Standard' then 2 else 3 end