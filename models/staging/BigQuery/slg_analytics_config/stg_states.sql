with states as (select * from {{ source('slg_analytics_config', 'src_states') }})

select
    id as state_id,
    state_name,
    cast(created_on as DATETIME) as created_on,
    cast(updated_on as DATETIME) as updated_on
from states