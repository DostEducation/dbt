select
    *
from {{ source ('unified_data_source','program_sequence') }}
where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE