select
    *
from {{source ('unified_data_source','content')}}
where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE