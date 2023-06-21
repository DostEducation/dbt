with state as (select * from {{ ref('stg_states') }}),
    district as (select * from {{ ref('stg_districts') }}),
    district_geographies as (
        select 
            district.*except(created_on, updated_on),
            state_name
        from state
        left join district using (state_id)
    )
select
    district_id,
    district_name, 
    state_id,
    state_name
from district_geographies