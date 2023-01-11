{{
    config(
        materialized='view'
    )
}}

with
    registration as (select * from {{ ref("stg_registration") }}),
    partner as (select * from {{ ref("stg_partner") }}),

    -- add deduplicated engagement level

    join_all_tables as (
        select *
        from
            registration
            left join partner using (partner_id)
    )

select *
from join_all_tables