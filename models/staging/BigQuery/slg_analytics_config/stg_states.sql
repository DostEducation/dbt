with states as (select * from {{ source('slg_analytics_config', 'src_states') }})

select
    id as state_id,
    state_name,
    created_on,
    updated_on
from states