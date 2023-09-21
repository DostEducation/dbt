with
  registrations_on_database as (
    select
    --   cast(registrations.user_created_on as date) as created_on,
      state_name,
      district_name,
      block_name,
      count(registrations.registration_uuid) as registration_on_database
    from
      {{ ref('stg_registration') }} registrations
    where
      true
      and registrations.user_created_on >= '2023-07-03'
    group by
      1, 2, 3
  ),
  registrations_on_app as (
    SELECT
      blocks.block_name,
      sum(coalesce(cast(centre_onboarding as int), 0) + coalesce(cast(home_onboarding as int), 0) + coalesce(cast(community_engagement_onboarded as int),0)) as reported_registrations
    FROM
      {{ ref('stg_blocks') }} blocks
      left join {{ ref('fct_activities') }} activities using (block_name)  
    GROUP BY
      1
  )

SELECT
  registrations_on_database.* except(block_name,registration_on_database,state_name),
  registrations_on_database.block_name as block_name_on_database,
  registration_on_database,
  registrations_on_app.*
FROM
  registrations_on_app
  full outer join registrations_on_database using (block_name)