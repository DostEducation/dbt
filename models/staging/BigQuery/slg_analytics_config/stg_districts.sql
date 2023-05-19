with district as (select * from {{ source('slg_analytics_config', 'src_districts') }}) 
select
    id as district_id,
    district_name,
    state_id,
    created_on,
    updated_on
from district