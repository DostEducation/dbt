with
    indicators as (select * from {{ ref('stg_indicators') }}),
    outcomes as (select * from {{ ref('stg_outcomes') }}),
    questions as (select * from {{ ref('stg_questions') }}),
    configured_responses as (select * from {{ ref('stg_configured_responses') }}),
    question_placement as (select * from {{ ref('stg_question_placement') }})

select * from outcomes