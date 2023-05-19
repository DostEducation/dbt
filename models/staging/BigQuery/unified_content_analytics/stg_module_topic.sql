with module_topic as (select * from {{ source('unified_content_analytics', 'module_topic') }})
select
    *
from module_topic