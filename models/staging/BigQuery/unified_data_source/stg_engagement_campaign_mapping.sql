with engagement_campaign_mapping as (
    select 
        *
    from {{ source("unified_data_source", "engagement_campaign_mapping") }}
)

select 
    *
from engagement_campaign_mapping