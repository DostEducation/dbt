with
    campaign as (
        select
            uuid as campaign_uuid,
            data_source,
            id as campaign_id,
            callsid,
            programseq_id as program_sequence_id,
            content_version_id,
            user_id,
        from {{ source("unified_data_source", "campaign") }}
        where
            migrated_on <= current_timestamp() - interval 100 minute
            and callsid <> ''
    )

select *
from campaign
