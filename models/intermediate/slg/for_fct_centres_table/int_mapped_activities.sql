with
    activities as (select * from {{ ref('stg_activities') }}),
    geographies as (select * from {{ ref('int_geographies') }})
    
select
    activities.*,
    state_name,
    district_name,
    block_name,
    sector_name,
    center_name
from
    activities
    left join geographies
        on case
            when activities.activity_level = 'State' then activities.state_id = geographies.state_id and geographies.activity_level = 'State'
            when activities.activity_level = 'District' then activities.district_id = geographies.district_id and geographies.activity_level = 'District'
            when activities.activity_level = 'Block' then activities.district_id = geographies.district_id and geographies.activity_level = 'Block'
        end
where activities.activity_level = 'Block'
    
