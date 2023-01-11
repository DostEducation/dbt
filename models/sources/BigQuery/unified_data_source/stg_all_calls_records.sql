select
    *
from {{ source('unified_data_source', 'all_call_records') }}
where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE