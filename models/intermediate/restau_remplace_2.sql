SELECT *,
    CASE 
        WHEN col1_filled IS NULL OR col1_filled = '' THEN d__tail_restaurant_2
        ELSE col1_filled
    END AS col1_filled_2
FROM {{ ref('restau_merged_jeff_bapt_abde') }}