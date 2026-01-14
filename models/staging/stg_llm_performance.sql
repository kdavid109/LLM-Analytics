{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'ai_models_performance') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim("Model") as model_name,
        trim("Creator") as creator,
        
        -- Context Window parsing
        case 
            when "Context Window" like '%M%' then 
                cast(replace(replace("Context Window", 'M', ''), ',', '') as float) * 1000000
            when "Context Window" like '%K%' then 
                cast(replace(replace("Context Window", 'K', ''), ',', '') as float) * 1000
            else cast(replace("Context Window", ',', '') as float)
        end as context_window_tokens,
        
        -- Intelligence Index
        cast(
            case 
                when "Intelligence Index" = 'N/A' then null
                else replace(replace("Intelligence Index", '$', ''), ',', '')
            end as float
        ) as intelligence_index,
        
        -- Price parsing
        cast(
            replace(replace("Price (Blended USD/1M Tokens)", '$', ''), ',', '')
            as decimal(10,2)
        ) as price_per_1m_tokens,
        
        -- Speed and Latency
        "Speed(median token/s)" as speed_tokens_per_sec,
        "Latency (First Answer Chunk /s)" as latency_seconds
        
    from source
),

add_calculated_fields as (
    select
        model_name,
        creator,
        context_window_tokens,
        intelligence_index,
        price_per_1m_tokens,
        speed_tokens_per_sec,
        latency_seconds,
        
        -- Intelligence to Price Ratio (higher is better value)
        case 
            when price_per_1m_tokens > 0 and intelligence_index is not null
            then intelligence_index / price_per_1m_tokens 
            else null 
        end as intelligence_price_ratio,
        
        -- Performance score combining intelligence and speed
        case 
            when intelligence_index is not null and speed_tokens_per_sec is not null
            then (intelligence_index * speed_tokens_per_sec)
            else null
        end as performance_score,
        
        -- Responsiveness (intelligence per latency)
        case 
            when latency_seconds > 0 and intelligence_index is not null
            then intelligence_index / latency_seconds
            else null 
        end as responsiveness_score

    from cleaned
)

select * from add_calculated_fields