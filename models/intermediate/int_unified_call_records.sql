with

    campaign as (
        select
            * except (campaign_uuid, campaign_id, callsid),
            campaign_uuid as unified_call_record_uuid,
            cast(callsid as string) as unified_call_id,
        from {{ ref("stg_campaign") }}
    ),

    call_log as (
        select
            * except (call_log_uuid, call_log_id),
            call_log_uuid as unified_call_record_uuid,
            cast(call_log_id as string) as unified_call_id,

        from {{ ref("stg_call_log") }}
    ),

    union_call_record_tables as (
        select * from campaign
        union all
        select * from call_log
    )

select *
from union_call_record_tables

