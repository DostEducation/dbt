select
    *
from {{ source('unified_data_source', 'ivr_prompt_response') }}