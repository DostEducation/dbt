with
  registrations_on_database as (
    select
      cast(registrations.user_created_on as date) as created_on,
      split(partners.partner_name, '-')[safe_ordinal(1)] as state_or_partner,
      split(partners.partner_name, '-')[safe_ordinal(2)] as district_name,
      split(partners.partner_name, '-')[safe_ordinal(3)] as block_name,
      count(registrations.registration_uuid) as registrations_on_database
    from
      {{ ref('stg_registration') }} registrations
      -- left join `growth_app_data.src_sectors` sectors on sectors.sector_name = registrations.sector
      left join {{ ref('stg_partner') }} partners
        on partners.partner_id = registrations.partner_id
          and partners.data_source = registrations.data_source
    where
      true
      and registrations.user_created_on >= '2023-07-03'
    group by
      1, 2, 3, 4
  ),
  registrations_on_app as (
    SELECT
      blocks.block_name,
      cast(activities.created_on as date) as created_on_app,
      sum(coalesce(cast(centre_onboarding as int), 0) + coalesce(cast(home_onboarding as int), 0) + coalesce(cast(follow_up as int), 0) + coalesce(cast(community_engagement_onboarded as int),0)) as reported_registrations
    FROM
      {{ ref('stg_blocks') }} blocks
      left join {{ ref('fct_activities') }} activities using (block_name)  
    GROUP BY
      1,2
  ),
  fix_block_names_on_database as (
    SELECT
      * EXCEPT (block_name),
      case
        when block_name = 'Vikasnagar' then 'Vikashnagar'
        when block_name = 'Sitargunj' then 'Sitarganj'
        when block_name = 'Dehradun City' then 'DDN City'
        when block_name = 'Baajpur' then 'Bazpur'
        else block_name
      end as block_name
    FROM
      registrations_on_database
  )

SELECT
  fix_block_names_on_database.* except(block_name,registrations_on_database,state_or_partner),
  fix_block_names_on_database.block_name as block_name_on_database,
  registrations_on_database,
  registrations_on_app.*
FROM
  registrations_on_app
  full outer join fix_block_names_on_database
    on registrations_on_app.created_on_app = fix_block_names_on_database.created_on
    and registrations_on_app.block_name = fix_block_names_on_database.block_name
where created_on_app >= '2023-07-03'