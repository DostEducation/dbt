with
    source as (select * from {{ source('slg_analytics_config', 'src_centres') }}),
    
    setup_columns as (
        select 
            id as centre_id,
            centre_name,
            sector_id,
            is_balvatika,
            safe_cast(total_beneficiaries as int) as total_beneficiaries,
            safe_cast(created_on as DATETIME) as created_on,
            safe_cast(updated_on as DATETIME) as updated_on
        from source
    )

select *
from setup_columns