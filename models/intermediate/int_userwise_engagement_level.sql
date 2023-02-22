with
    source as (select * from {{ ref("int_dedupe_user_engagement") }}),

    identify_program_with_max_calls_delivered as (
        select
            *,
            row_number() over (
                partition by user_id, data_source order by delivered_calls_count desc
            ) as row_number
        from source
    ),

    get_program_with_max_calls_delivered as (
        select * from identify_program_with_max_calls_delivered
        where row_number = 1
    )

select *
from get_program_with_max_calls_delivered
