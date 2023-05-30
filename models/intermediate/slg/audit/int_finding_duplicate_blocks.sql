with blocks as (select * from {{ ref('stg_blocks') }}),
    finding_duplicates as (
        select 
            block_id,
            block_name,
            count(block_id) as no_of_blocks
        from blocks
        group by 1,2
    )
select 
    *
from finding_duplicates
