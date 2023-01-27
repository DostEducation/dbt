with call_records as (select * from {{ ref("stg_all_call_records") }}),
    content_version as (select * from {{ ref("stg_content_version") }}),
    language_used  as (select * from {{ ref("stg_language") }}),
    users as (select * from {{ ref("stg_users") }}),
    partner as (select * from {{ ref("stg_partner") }}),

    joining_all_tables as (
    
        SELECT 
            call_records.*,
            content_version.content_duration as content_version_duration,
            language_used.language_name,
            language_used.language_id,
            partner.partner_name
        FROM  call_records 
        left join content_version using (content_version_id, data_source)
        left join language_used using(language_id, data_source)
        left join users using (user_id, data_source)
        left join partner using(partner_id, data_source)
    )

select 
    *
from joining_all_tables