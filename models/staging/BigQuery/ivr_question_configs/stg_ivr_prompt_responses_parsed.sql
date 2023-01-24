with prompt_responses as (
    select  
        *
    from {{ source("ivr_question_configs", "ivr-prompt-responses-parsed") }}
)
select
    *
from prompt_responses