with program_sequence as (
    select
        id as program_sequence_id,
        data_source,
        program_id,
        module_id,
        content_id
    from {{ source ("unified_data_source","program_sequence") }}
    where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE
)

select * from program_sequence