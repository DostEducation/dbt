WITH modules as (select * from {{ ref('stg_module') }}),
  modules_with_topics as (
    SELECT
      module_name,
      substr(module_name, 5) as number_plus_name,
      case
        when module_name = 'INTRO_1-SIGNUP' then 0
        when substr(module_name, 6, 1)= '-' then safe_cast(substr(module_name, 5, 1) as int)
        else safe_cast(substr(module_name, 5, 2) as int)
      end as topic_number, 
      case
        when module_name = 'INTRO_1-SIGNUP' then substr(module_name, 9)
        when substr(module_name, 6, 1)= '-' then substr(module_name, 7)
        else substr(module_name, 8)
      end as topic
    FROM modules
      
    WHERE
      data_source = 'rp_ivr'
  )

SELECT
  topic_number,
  topic,
  initcap(topic) as topic_capitalized
FROM
  modules_with_topics
GROUP BY
  1, 2
ORDER BY
  1