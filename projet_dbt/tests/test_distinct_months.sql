WITH months AS (
    SELECT DISTINCT DATE_TRUNC('month', tpep_pickup_datetime) AS month
    FROM {{ ref('transform') }}
)

SELECT COUNT(*) AS distinct_months_count
FROM months
HAVING COUNT(*) <> 12

