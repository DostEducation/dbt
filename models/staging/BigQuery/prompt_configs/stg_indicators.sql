with
    indicators as (
    select
        safe_cast(id as integer) as indicator_id,
        indicator_name,
        safe_cast(outcome_id as integer) as outcome_id,
        type as indicator_type
    from {{ source("prompt_configs", "src_indicators") }}
    )

select * from indicators