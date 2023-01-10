select
    *
from {{ source ('prompt_configs', 'src_questions')}}