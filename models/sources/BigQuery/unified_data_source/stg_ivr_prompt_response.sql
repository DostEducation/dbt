select
    *
from {{ source('unified_data_source', 'ivr_prompt_response') }}
where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE