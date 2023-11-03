with
    dates as (select * from {{ ref('int_dates') }}),
    geographies as (select * from {{ ref('int_geographies') }}),

    cross_join_dates as (
        select * 
        from dates 
        cross join geographies
    )

select
    *
from cross_join_dates