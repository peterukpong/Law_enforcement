with source as (

    select * from {{ ref('int_law_dimensional_model') }}

)

SELECT
    EXTRACT(YEAR FROM received_datetime) AS year,
    EXTRACT(MONTH FROM received_datetime) AS month,
    COUNT(*) AS call_count
FROM
    {{ ref('int_law_fact_model') }}
GROUP BY
    year,
    month
ORDER BY
    year,
    month