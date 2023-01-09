select
    *
from {{ source('unified_data_source', 'all_call_records') }}