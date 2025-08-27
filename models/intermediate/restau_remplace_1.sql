SELECT
    CASE 
        WHEN new_cat_cleaned IS NULL OR new_cat_cleaned = '' THEN Type_principal_normal
        ELSE new_cat_cleaned
    END AS col1_filled,
    Type_principal_normal
FROM {{ ref('restau_merged_jeff_bapt') }}