WITH temp AS (
SELECT
  *,
  CASE 
    WHEN LOWER(TRIM(Type_principal)) IN (
      'restaurant de sushis', 'restaurant de nouilles (ramen)', 'restaurant de nouilles',
      'restaurant de porc pané et frit (tonkatsu)', 'restaurant de type izakaya', 
      'restaurant japonais régional', "restaurant d'omelettes japonaises (okonomiyaki)", 
      'restaurant à plaque chauffante (teppanyaki)', 'restaurant japonais authentique'
    ) THEN 'Restaurant japonais'
    
    WHEN LOWER(TRIM(Type_principal)) IN (
      'restaurant mandarin', 'restaurant hot pot', 'restaurant de dimsums',
      'restaurant de spécialités du sichuan (chine)', 
      'restaurant wok', 'restauration rapide style hong kong', 'restaurant servant des nouilles chinoises'
    ) THEN 'Restaurant chinois'
    
    WHEN LOWER(TRIM(Type_principal)) IN ('bar à poke') THEN 'Restaurant hawaïen'

    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de soupe vietnamienne « pho »') THEN 'Restaurant vietnamien'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant servant des pancakes', 'restaurant pour le déjeuner', 'restaurant servant le petit-déjeuner') THEN 'Restaurant brunch'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de hot-dogs', "restaurant de grillades à l'américaine", 'restaurant californien', 'restaurant cajun (états-unis)') THEN 'Restaurant américain'

    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant tex-mex (mexique)') THEN 'Restaurant mexicain'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant sri-lankais') THEN 'Restaurant indien'


    
    WHEN LOWER(TRIM(Type_principal)) IN (
      'brasseries', 'brasserie', 'restaurant de spécialités alsaciennes', 'bistro',
      'restaurant de grillades à la française', 'restaurant basque', 
      'cuisine française moderne', 'restaurant de fondues', 'restaurant de cuisine de campagne', 'restaurant de spécialités du sud-ouest de la france', 'restaurant français gastronomique', 'restaurant servant de la raclette'
    ) THEN 'Restaurant français'
    
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de grillades coréennes') THEN 'Restaurant coréen'

    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant indien musulman', 'restaurant indien moderne') THEN 'Restaurant indien'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de tapas') THEN 'Bar à tapas'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant servant du couscous') THEN 'Restaurant marocain'
    WHEN LOWER(TRIM(Type_principal)) IN ('pub (cuisine gastronomique)') THEN 'Restaurant gastronomique'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant spécialisé dans les falafels') THEN 'Restaurant libanais'
    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de chawarmas') THEN 'Restaurant turc'
    WHEN LOWER(TRIM(Type_principal)) IN ("café d'art") THEN 'Café'
    WHEN LOWER(TRIM(Type_principal)) IN ("restaurant d'asie du sud-est", 'restaurant de spécialités asiatiques') THEN 'Restaurant asiatique'
    WHEN LOWER(TRIM(Type_principal)) IN ("restaurant de spécialités d'afrique de l'ouest") THEN 'Restaurant africain'


    WHEN LOWER(TRIM(Type_principal)) IN ('restaurant de grillades','restaurant de volaille', 'grillades', 'restaurant spécialisé dans les ailes de poulet', 'restaurant à viande') THEN 'Restaurant de viande'


    
    WHEN LOWER(TRIM(Type_principal)) IN (
      "restaurant de spécialités d'italie du sud", 
      'restaurant de spécialités de la région de naples', 
      'restaurant sicilien'
    ) THEN 'Restaurant italien'
    
    ELSE Type_principal
  END AS Type_principal_normal
FROM {{ ref('stg_RESTAUPARIS__testbapt5') }}
  WHERE LOWER(TRIM(Type_principal)) NOT IN ('agence événementielle', 'arrêt de bus', 'cabaret', 'café spécialisé dans les boissons expresso', 'café servant des chocolats chauds','charcuterie', 'espace de coworking', 'fournisseur de paniers-repas', 'kiosque','magasin de gâteaux', 'magasin vendant du poulet', 'marchand de bière', 'marchand de cookies', 'marchand de pâtes', "organisateur d'événements",'organisme à but non lucratif', 'primeur', 'restaurant de plats à base de riz', 'restaurant de spécialités à base de poisson-pêcheur', 'restaurant self-service', 'salle de fête', 'service de blanchisserie','service de voiturier','supermarché','sushi à emporter','vin','épicerie indienne', 'épicerie japonaise', 'artisan chocolatier','bar pmu', 'bar karaoké','bar salsa','bar à jus de fruits', 'boîte de nuit', 'cuisine professionnelle partagée', 'magasin de thé','poissonnerie','restaurant de desserts', 'bar gay', 'boutique de desserts', 'chocolatier', 'livraison de pizzas', 'piano-bar', 'restaurant casher','restaurant chinois à emporter','cafétéria','magasin de thé aux perles','restaurant de type buffet', 'restaurant diététique', 'restaurant familial', 'bureau de tabac', 'casino', 'pizza à emporter', 'pâtisserie', 'restaurant biologique', 'épicerie coréenne', 'bar avec musique live', 'microbrasserie', 'bar sportif', 'cave à vins', 'livraison de repas à domicile', 'plats chinois à emporter', 'salle de réception', 'épicerie fine', 'marchand de glaces', 'restaurant à emporter', 'épicerie asiatique','bar à bières','épicerie','caviste','bar à chicha', 'pâtisseries', 'pub','salon de thé','bar-tabac','épicerie italienne', 'boulangerie','magasin de café','bar à vin', 'bar à cocktails', 'restaurant halal', 'bar lounge', 'restaurant', 'bar', 'restauration rapide', 'café', 'traiteur','restaurant brunch', 'restaurant gastronomique', 'bar à tapas','dîner', 'restaurant juif','restaurant de spécialités perses','pub irlandais','deli','restaurant de tacos','marchand de bagels','diner')
)

SELECT
*
FROM temp