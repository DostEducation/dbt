with prompt_responses as (
    select  
        
        count(id)
    from {{ source("ivr_question_configs", "ivr-prompt-responses-parsed") }}
)
select
    *
from prompt_responses
where keypress is null