with centres as (select * from {{ source('slg_analytics_config', 'src_centres') }}),
    structuring_centres as (
        select 
            id as centre_id,
            centre_name,
            state_id,
            district_id,
            block_id,
            sector_code as sector_id,
            total_beneficiaries,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from centres
    )
select
    *
from structuring_centres