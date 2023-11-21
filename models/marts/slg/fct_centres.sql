with
    activities as (select * from {{ ref('fct_activities') }} where activity_level = 'Centre' and activity_type = 'Centre Visit Onboarding'),
    geography as (select * from {{ ref('int_geographies') }} where activity_level = 'Centre'),
    
    join_georaphies_and_activities as (
        select
            geography.*,
            date_of_meeting,
            if(date_of_meeting is not null, true, false) as centre_visited_y_n
        from geography
            left join activities using (centre_id)
        where geography.activity_level = 'Centre'
    ),

    add_row_number_by_date_of_meeting as (
        select
            *,
            row_number() over (
                partition by centre_id order by date_of_meeting desc
            ) as row_number
        from join_georaphies_and_activities
    ),
    select_latest_visit_record as (
        select *
        from add_row_number_by_date_of_meeting
        where row_number = 1
    )

select
    *
from select_latest_visit_record
