with registration as (select * from {{ ref('stg_registration') }}),
    partner as (select * from {{ ref('stg_partner') }}),
    adding_block_name_registration as (
    select
      registration.*,
      split(partner.partner_name, '-')[safe_ordinal(1)] as state_or_partner,
      split(partner.partner_name, '-')[safe_ordinal(2)] as district,
      split(partner.partner_name, '-')[safe_ordinal(3)] as block_name,
    from registration
    left join partner using (partner_id, data_source)
    where
      true
      and registration.user_created_on >= '2022-12-05'
    group by
      1, 2, 3
    )
select
    *
from adding_block_name_registration