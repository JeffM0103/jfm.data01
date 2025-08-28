WITH temp AS(
SELECT *,
  CASE 
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant ouzbek', 'restaurant cambodgien', 'restaurant malaisien', 'restaurant taïwanais', 'restaurant indonésien','restaurant de cuisine fusion asiatique' ) THEN 'Restaurant asiatique'
    WHEN LOWER(TRIM(col1_filled_2)) IN (
      'restaurant de cuisine européenne moderne', 'restaurant arménien', 'restaurant suédois', 'restaurant danois','restaurant finlandais','restaurant géorgien','restaurant roumain','restaurant russe', 'restaurant portugais') THEN 'Restaurant européen'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant latino-américain', 'restaurant cubain','restaurant vénézuélien','restaurant uruguayen','restaurant péruvien', 'restaurant chilien') THEN 'Restaurant latino américain'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant croate', 'restaurant syrien') THEN 'Restaurant méditerranéen'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant québécois', 'restaurant canadien') THEN 'Restaurant américain'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant népalais', 'restaurant bangladais', 'restaurant pakistanais') THEN 'Restaurant indien'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant grec') THEN 'Restaurant grec'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant hawaïen') THEN 'Restaurant hawaien'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant thaï') THEN 'Restaurant thai'
    WHEN LOWER(TRIM(col1_filled_2)) IN ("restaurant de spécialités d'afrique du nord") THEN 'Restaurant africain'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant de hamburgers') THEN 'Restaurant à hamburgers'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant libanais') THEN 'Restaurant libanais'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant turc') THEN 'Restaurant turc'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('bar-restaurant à huîtres') THEN 'Restaurant de fruits de mer'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('restaurant haïtien', 'restaurant caribéen') THEN 'Restaurant créole'
    WHEN LOWER(TRIM(col1_filled_2)) IN ('bar à soupes','café','bar à tapas','restaurant sans gluten','restaurant gastronomique', 'restaurant de spécialités du moyen-orient', 'restaurant de cuisine fusion','restaurant brunch','restaurant perse', 'restaurant australien', 'restaurant israélien', 'restaurant végétalien','restaurant végétarien', 'restaurant de viande', '0') THEN NULL
    
    ELSE col1_filled_2
  END AS col1_filled_3
FROM `restauparis.dbt_bviu.restau_remplace_2` 
)

SELECT *
FROM temp