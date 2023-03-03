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
        select
            *,
            if(data_source = 'admindashboard', question_placement.program_sequence_id, question_placement.content_id) as unified_content_map_id,
            if(evaluation_phase = 'Baseline', question_id, parent_question_id) as baseline_question_id
        from
            questions
            left join configured_responses using (question_id)
            left join question_placement using (question_id)
            left join indicators using (indicator_id)
            left join outcomes using (outcome_id)
        where
            question_placement_status <> 'NotDeployed' -- to avoid duplicate records
            and (
                question_placement_id <= 324
                or question_placement_id >= 345
            ) -- to filter out instances where more than 1 prompt are placed
    )

select *
from join_all_tables
where unified_content_map_id is not null -- to hand instances where neither program_seq_id nor content_id have been supplied
