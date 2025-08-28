SELECT 
actif,
count(actif) as nb 
FROM {{ ref('restau_merged_INSEE') }}
GROUP BY actif