select
    *
from {{ source ('prompt_configs', 'src_question_placement')}}