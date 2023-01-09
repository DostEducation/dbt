select
    *
from {{ source ('unified_data_source','program_sequence') }}