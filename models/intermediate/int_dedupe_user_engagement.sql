with
    program_wise_engagement as (select * from  {{ ref("stg_program_wise_engagement") }}),

    engagement_level_values as (
        select *
        from
            unnest(
                [
                    struct<level string, level_value int64>('low', 1),
                    ('medium', 2),
                    ('high', 3)
                ]
            )
    ),

    pwel_coded as (
        select
            data_source,
            user_id,
            program_name,
            total_unique_content_answered,
            total_unique_content_delivered,
            first_user_week_engagement_level,
            fu_week_el.level_value as first_user_week_engagement_level_value,
            first_user_month_engagement_level,
            fu_month_el.level_value as first_user_month_engagement_level_value,
            overall_engagement_level,
            overall_el.level_value as overall_engagement_level_value,
            program_start_date,
            program_end_date,
            user_age,
            delivered_calls_count,
            total_corrected_listen_seconds,
            total_content_duration
        from program_wise_engagement
        left join
            engagement_level_values fu_week_el
                on fu_week_el.level = first_user_week_engagement_level
        left join
            engagement_level_values fu_month_el
                on fu_month_el.level = first_user_month_engagement_level
        left join
            engagement_level_values overall_el
                on overall_el.level = overall_engagement_level
        where
            program_name is not null
    ),

    pwel_deduplicated as (
        select
            data_source,
            user_id,
            program_name,
            max(total_unique_content_delivered) as total_content_delivered,
            max(total_unique_content_answered) as total_content_answered,
            max(total_corrected_listen_seconds) as total_listen_seconds,
            max(total_content_duration) as total_content_duration,
            max(
                first_user_week_engagement_level_value
            ) as first_user_week_engagement_level_value,
            max(
                first_user_month_engagement_level_value
            ) as first_user_month_engagement_level_value,
            max(overall_engagement_level_value) as overall_engagement_level_value,
            min(program_start_date) as program_start_date,
            max(program_end_date) as program_end_date,
            max(user_age) as user_age,
            max(delivered_calls_count) as delivered_calls_count
        from pwel_coded
        group by 1, 2, 3
    )

select
    pwel_deduplicated.*,
    program.id as program_id,
    fu_week_el.level as first_user_week_engagement_level,
    fu_month_el.level as first_user_month_engagement_level,
    overall_el.level as overall_engagement_level
from pwel_deduplicated
left join
    engagement_level_values fu_week_el
    on fu_week_el.level_value = pwel_deduplicated.first_user_week_engagement_level_value
left join
    engagement_level_values fu_month_el
    on fu_month_el.level_value
    = pwel_deduplicated.first_user_month_engagement_level_value
left join
    engagement_level_values overall_el
    on overall_el.level_value = pwel_deduplicated.overall_engagement_level_value
left join
    `unified_data_source.program` program
    on program.name = pwel_deduplicated.program_name
