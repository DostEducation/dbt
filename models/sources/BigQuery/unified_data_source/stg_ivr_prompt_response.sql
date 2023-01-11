with
    ivr_prompt_response as (

        select
            id as prompt_response_id,
            uuid as prompt_response_uuid,
            data_source,
            keypress,
            if(
                data_source = 'rp_ivr',
                cast(call_log_id as string),
                cast(call_sid as string)
            ) as unified_call_id,
            datetime(
                ivr_prompt_response.created_on, 'Asia/Kolkata'
            ) as ivr_prompt_response_created_on,
            response as webhook_response_value
        from {{ source("unified_data_source", "ivr_prompt_response") }}
        where migrated_on <= current_timestamp() - interval 100 minute

    )

select *
from ivr_prompt_response
