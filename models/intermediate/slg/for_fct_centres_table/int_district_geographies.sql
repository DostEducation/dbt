with state as (select * from {{ ref('stg_states') }}),
    district as (select * from {{ ref('stg_districts') }}),
    district_geographies as (
        select 
            district.*except(created_on, updated_on),
            state_name
        from district
        left join state using (state_id)
    )
select
    state_id,
    state_name,
    district_id,
    district_name 
from district_geographies