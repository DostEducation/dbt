with
    activities as (select * from {{ ref('stg_activities') }}),
    geographies as (select * from {{ ref('int_geographies') }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),
    joining_all_geographies as (
        select
            activities.*,
            state_name,
            district_name,
            block_name,
            sector_name,
            centre_name
        from
            activities
            left join geographies
                on case
                    when activities.activity_level = 'State' then activities.state_id = geographies.state_id and geographies.activity_level = 'State'
                    when activities.activity_level = 'District' then activities.district_id = geographies.district_id and geographies.activity_level = 'District'
                    when activities.activity_level = 'Block' then activities.block_id = geographies.block_id and geographies.activity_level = 'Block'
                    when activities.activity_level = 'Sector' then activities.sector_id = geographies.sector_id and geographies.activity_level = 'Sector'
                    when activities.activity_level = 'Centre' then activities.centre_id = geographies.centre_id and geographies.activity_level = 'Centre'
                end
    )
    select
        joining_all_geographies.*,
        dost_member_name,
        role
    from joining_all_geographies
    left join dost_team using (dost_team_id)
    
