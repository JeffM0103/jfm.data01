SELECT
a.*,
b.d__tail_restaurant_2
FROM {{ ref('restau_remplace_1') }} as a
LEFT JOIN {{ ref('stg_RESTAUPARIS__cat_restau_abdellah') }} as b
ON lower(a.adresse_complete) = lower(b.adresse_complete)