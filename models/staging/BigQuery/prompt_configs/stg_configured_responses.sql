with
    source as (
        select * from {{ source("prompt_configs", "src_configured_responses") }}
    ),

    renamed as (
        select
            safe_cast(id as integer) as response_id,
            safe_cast(question_id as integer) as question_id,
            safe_cast(keypress as integer) as keypress,
            response_eng as response_english,
            -- response_hin,
            desired_response,
            webhook_resp_val as webhook_response_value
        from source
    )


select
    *
from renamed
