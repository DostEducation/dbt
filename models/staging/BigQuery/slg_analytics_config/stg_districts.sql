with district as (select * from {{ source('slg_analytics_config', 'src_districts') }}) 
select
    id as district_id,
    district_name,
    state_id,
    cast(created_on as DATETIME) as created_on,
    cast(updated_on as DATETIME) as updated_on
from district