with call_records_content_ver_lang as (

    SELECT 
    acr.*,
    c.content_duration as content_version_duration,
    l.language_name,
    l.language_id 
    FROM  {{ ref("stg_all_call_records") }} acr 
    left join {{ ref("stg_content_version") }} c on c.content_version_id = acr.content_version_id
    left join {{ ref("stg_language") }} l on c.language_id = l.language_id
)
select 
    *
from call_records_content_ver_lang