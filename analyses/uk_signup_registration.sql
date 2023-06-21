WITH user_details AS (
    SELECT 
    RIGHT(reg.user_phone, 10) AS user_phone,
    reg.user_id,
    DATETIME(reg.user_created_on, 'Asia/Kolkata') as user_registered_on,
    partner.partner_name AS partner_name,
    DATETIME(prompt_response.prompt_timestamp, 'Asia/Kolkata') AS user_signed_up_to_uk_program_on
    FROM {{ ref('stg_registration') }} AS reg
    LEFT JOIN {{ ref('stg_partner') }} AS partner
        ON reg.partner_id = partner.partner_id
        AND reg.data_source = 'admindashboard'
        AND reg.data_source = partner.data_source
    LEFT JOIN {{ ref('stg_ivr_prompt_response') }} as prompt_response
        ON reg.data_source = 'admindashboard'
        AND prompt_response.data_source= reg.data_source
        AND RIGHT(prompt_response.user_phone, 10) = RIGHT(reg.user_phone, 10)
        AND prompt_response.response LIKE '%PROGRAM-OPTIN%'
        AND (prompt_response.response LIKE '%B-3%'
        OR prompt_response.response LIKE '%T-6%')
), partitioned_users AS (
    SELECT *, ROW_NUMBER() 
    OVER(PARTITION BY user_phone order by user_signed_up_to_uk_program_on) AS row_number
    FROM user_details
)
SELECT * FROM partitioned_users 
WHERE row_number = 1
