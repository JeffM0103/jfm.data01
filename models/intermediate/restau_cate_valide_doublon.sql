{{ config(materialized='table') }}

WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY adresse_complete
            ORDER BY adresse_complete
        ) AS rn
    FROM {{ ref('restau_cate_valide') }}
)
SELECT *
FROM ranked
WHERE rn = 1
