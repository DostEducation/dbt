with activities as (select * from {{ ref('stg_activities') }}),
    team_member as (select * from {{ ref('stg_dost_team') }}),
    joining_tables as (
        select
            team_member.dost_team_id,
            team_member.name,
            count(activities.dost_team_id) as no_of_activities
        from team_member
        left join activities using (dost_team_id)
        where team_member.role in ('dc', 'dl', 'ops_team')
        and activities.created_on >= CURRENT_DATE() - INTERVAL 7 DAY
        group by 1,2
    ),
    members_reported_activities as (
        select 
            name
        from team_member
        where team_member.role in ('dc', 'dl','ops_team')
    )

select
    count(distinct joining_tables.name)/count(distinct members_reported_activities.name) as percent_reported
from members_reported_activities
left join joining_tables using (name)