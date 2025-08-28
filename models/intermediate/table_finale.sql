SELECT
r.*,
i.restauration_rapide,
i.cuisine_fusion,
i.brasserie,
i.tapas,
i.bar,
i.date_creation,
i.actif,
i.employeur,
i.anciennete,
i.geoloc_restau,
i.geometry_quartier,
i.geoloc_quartier,
i.quartier
FROM {{ ref('restau_carac_added') }} AS r
LEFT JOIN {{ ref('restau_merged_quartier') }} AS i
ON LOWER(r.adresse_complete) = LOWER(i.adresse_complete)