WITH restau_quartier AS ( 
  SELECT *,
  -- Nettoyage des avis et photos (remplacement null par 0)
  COALESCE(nombre_d_avis, 0) AS nb_avis,
  COALESCE(nombre_de_photos, 0) AS nb_photos,
  COALESCE(note_avis, 0) AS note,

  -- Concurrence par quartier et type
  COUNT(nom_adresse) OVER (PARTITION BY quartier) AS nb_restau_quartier,
  COUNT(nom_adresse) OVER (PARTITION BY quartier, type) AS nb_restau_type_quartier
  FROM {{ ref('table_finale') }}
  WHERE nom_adresse IS NOT NULL
  )
  ,
  max_values AS (
  SELECT *,
  MAX(nb_avis) OVER() AS max_avis,
  MAX(nb_photos) OVER() AS max_photos,
  MAX(nb_restau_quartier) OVER () AS max_nb_restau_quartier,
  MAX(nb_restau_type_quartier) OVER () AS max_nb_restau_type_quartier
  FROM restau_quartier)
  ,
  
  score AS (
  SELECT
  *,
  -- Popularité normalisée
  (LOG(1 + LOG(1+nombre_d_avis)) / LOG(1 + log(1+max_avis)) + LOG(1 + log(1+nombre_de_photos)) / LOG(1 + log(1+max_photos)) ) / 2 AS popularite,
  -- Satisfaction pondérée
  (note / 5) * (LOG(1 + log(1+log(1+nombre_d_avis))) / LOG(1 + log(1+log(1+max_avis)))) AS satisfaction, -- Visibilité digitale
  ( CAST(lien_facebook AS INT64) + CAST(lien_instagram AS INT64) + CAST(site_internet AS INT64) + CAST(liens_de_resa AS INT64) ) / 4.0 AS visibilite,
  -- Concurrences par quartier
  -- saturation marché
  LOG(1 + nb_restau_quartier) / LOG(1 + max_nb_restau_quartier) AS concurrence_quartier,
  -- concurrence par type de cuisine
  LOG(1 + nb_restau_type_quartier) / LOG(1 + max_nb_restau_type_quartier) AS concurrence_type_cuisine
  FROM max_values )
  
  SELECT
  * EXCEPT(popularite,satisfaction,visibilite,concurrence_quartier,concurrence_type_cuisine,max_avis,max_nb_restau_quartier,max_nb_restau_type_quartier,nombre_de_photos,nombre_d_avis,note_avis),
  round(popularite,2) as score_popularite,
  round(satisfaction,2) as score_satisfaction,
  round(visibilite,2) as score_visibilite,
  round(concurrence_quartier,2) as concurrence_quartier,
  round(concurrence_type_cuisine,2) as concurrence_type_cuisine,
  -- Score final valorisant réussite en zone concurrentielle
  ROUND((0.5 * popularite + 0.4 * satisfaction + 0.1 * visibilite) * (1+0.05*(0.5*concurrence_quartier + 1.5*concurrence_type_cuisine)/2), 4) AS score_reussite
  FROM score
--where nom_adresse like "%pink mamma%" OR nom_adresse like "%crmaillre%" OR nom_adresse like'%vege brancion%' OR nom_adresse like "%bouillon chartier%" or nom_adresse like '%alan turing%' or nom_adresse like "%chez lu%"
  order by score_reussite desc
 -- limit 100

