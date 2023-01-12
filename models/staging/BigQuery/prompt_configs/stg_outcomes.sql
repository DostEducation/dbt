with
    outcomes as (
    select
        safe_cast(id as integer) as outcome_id,
        spectrum,
        outcome_name
    from {{ source("prompt_configs", "src_outcomes") }}
    )

select * from outcomes