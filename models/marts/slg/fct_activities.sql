with activities as (select * from {{ ref('int_activities') }})

select
    *
from activities
