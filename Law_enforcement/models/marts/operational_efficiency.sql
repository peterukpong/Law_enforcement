-- Query to analyze operational efficiency
WITH source AS (
    SELECT DISTINCT id, agency
    FROM {{ ref('int_law_dimensional_model') }} AS dim
)

SELECT
    dim.agency,
    AVG(enroute_time) AS avg_enroute_time,
    AVG(close_time) AS avg_close_time
FROM source dim
LEFT JOIN (
    SELECT
        id,
        (enroute_datetime - received_datetime) AS enroute_time,
        (close_datetime - received_datetime) AS close_time
    FROM {{ ref('int_law_fact_model') }} AS fact
) subquery
ON dim.id = subquery.id
GROUP BY dim.agency
ORDER BY avg_enroute_time
