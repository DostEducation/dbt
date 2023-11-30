with
    date_spine as (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2019-01-01' as date)",
                end_date="current_date() + 1",
            )
        }}
    ),

    format_columns as (
        select
            cast(date_day as date) as date
        from date_spine
    )

select *
from format_columns