with
    source as (select * from {{ ref('stg_question_placement') }}),

    add_count_by_content_map_id as (
        select
            *,
            count(unified_content_map_id) over (partition by unified_content_map_id, data_source) as count
        from source
        where
            question_placement_status = 'Active'
            or
                (question_placement_status = 'Inactive' and question_placement_deactivation_date is null)
    ),

    filter_content_map_ids_with_multiple_placements as (
        select * from add_count_by_content_map_id
        where count >= 2
    )

select *
from filter_content_map_ids_with_multiple_placements
where
    true
    -- and question_placement_status = 'Inactive'
    -- and question_placement_deactivation_date is null
    -- and unified_content_map_id = '1367'
order by unified_content_map_id
