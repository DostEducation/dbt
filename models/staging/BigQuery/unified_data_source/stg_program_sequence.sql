with program_sequence as (
    select
        id as program_sequence_id,
        data_source,
        program_id,
        module_id,
        content_id,
        cast(if(data_source = 'admindashboard', id, content_id) as string) as unified_content_map_id,
    from {{ source ("unified_data_source","program_sequence") }}
    where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE
)

select * from program_sequence