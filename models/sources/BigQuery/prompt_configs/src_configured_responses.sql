select
    *
from {{ source ('prompt_configs', 'src_configured_responses')}}