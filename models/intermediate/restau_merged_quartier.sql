SELECT
    r.*,
    DATE_DIFF(DATE '2025-08-15', DATE(date_creation), YEAR) AS anciennete,
    CONCAT(CAST(r.latitude AS FLOAT64), ',', CAST(r.longitude AS FLOAT64)) AS geoloc_restau,
    q.Geometry as geometry_quartier,
    Geometry_X_Y  as geoloc_quartier,
    q.L_QU AS quartier
FROM {{ ref('restau_merged_INSEE') }} r
JOIN restauparis.RESTAUPARIS.QuartiersParis q
  ON ST_CONTAINS(
    ST_GEOGFROMGEOJSON(q.Geometry),
    ST_GEOGPOINT(r.longitude, r.latitude)
  )
