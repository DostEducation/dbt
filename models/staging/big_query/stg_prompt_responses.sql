with
    prompt_responses as (
    select
        *
    from {{ source("unified_data_source", "ivr_prompt_response") }} limit 10
    )

select * from prompt_responses