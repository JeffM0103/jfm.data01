SELECT
r.*,
i.Type_principal_normal
FROM {{ ref('stg_RESTAUPARIS__RestauParis_new_cat5') }} as r
LEFT JOIN {{ ref('int_bapt_categories') }} as i
ON r.adresse_complete = lower(i.adresse_complete)