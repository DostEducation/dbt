-- change materialzation to table

{{
    config(
        materialized='table'
    )
}}

with
    indicators as (select * from {{ ref("stg_indicators") }}),
    outcomes as (select * from {{ ref("stg_outcomes") }}),
    questions as (select * from {{ ref("stg_questions") }}),
    configured_responses as (select * from {{ ref("stg_configured_responses") }}),
    question_placement as (select * from {{ ref("stg_question_placement") }}),

    join_all_tables as (
        select *
        from
            questions
            left join configured_responses using (question_id)
            left join question_placement using (question_id)
            left join indicators using (indicator_id)
            left join outcomes using (outcome_id)
    )

select *
from join_all_tables
