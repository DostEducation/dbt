with enrolled_users as (select * from {{ source('unified_data_source', 'unified_data_source_for_enrolled_users') }}),
    unified_enrolled_users as (
        select 
            *
        from enrolled_users
    )
select 
    *
from unified_enrolled_users
where data_source = "admindashboard"