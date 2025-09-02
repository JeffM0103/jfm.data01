SELECT
    INITCAP(
            REGEXP_REPLACE(
                TRANSLATE(
                    lower(nom),
                    'àâäáãåçèéêëìíîïñòóôöõùúûüýÿÀÂÄÁÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ',
                    'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY'
                ),
                r'[^a-zA-Z0-9 ]', ' '                     -- supprime caractères spéciaux
            )
        ) as nom,
description,
ouvert,
col1_filled_3 as type,
adresse_complete,
adresse_1,
code_postal,
longitude,
latitude,
gamme_de_prix,
nombre_d_avis,
note_avis,
tags_des_avis,
nombre_de_photos,
caracteristiques,
telephone,
site_internet,
email,
lien_facebook,
lien_instagram,
liens_de_resa,
liens_de_commande,
Lien_maps,

  -- 🌐 SERVICE OPTIONS
  JSON_VALUE(Caracteristiques, '$.service-options.delivery') = 'offers-delivery' AS has_delivery,
  JSON_VALUE(Caracteristiques, '$.service-options.takeout') = 'offers-takeout' AS has_takeout,
  JSON_VALUE(Caracteristiques, '$.service-options.outdoor-seating') = 'has-outdoor-seating' AS has_terrasse,

  -- 🥂 HIGHLIGHTS
  JSON_VALUE(Caracteristiques, '$.highlights.rooftop-seating') = 'has-rooftop-seating' AS has_rooftop_seating,

  -- ♿ ACCESSIBILITY
  JSON_VALUE(Caracteristiques, '$.accessibility.wheelchair-accessible-entrance') = 'has-wheelchair-accessible-entrance' AS wheelchair_access_entrance,

  -- 🍷 OFFERINGS
  JSON_VALUE(Caracteristiques, '$.offerings.alcohol') = 'serves-alcohol' AS serves_alcohol,
  JSON_VALUE(Caracteristiques, '$.offerings.cocktails') = 'serves-cocktails' AS serves_cocktails,
  JSON_VALUE(Caracteristiques, '$.offerings.vegan-options') = 'serves-vegan-dishes' AS serves_vegan,
  JSON_VALUE(Caracteristiques, '$.offerings.vegetarian-options') = 'serves-vegetarian-dishes' AS serves_vegetarian,

  -- 🍽 DINING OPTIONS
  JSON_VALUE(Caracteristiques, '$.dining-options.brunch') = 'serves-brunch' AS serves_brunch,
  JSON_VALUE(Caracteristiques, '$.dining-options.seating') = 'has-seating' AS has_seating,

  -- 🏠 AMENITIES
  JSON_VALUE(Caracteristiques, '$.amenities.bar-onsite') = 'has-bar-onsite' AS has_bar_onsite,
  JSON_VALUE(Caracteristiques, '$.amenities.wi-fi') = 'has-wi-fi' AS has_wifi,

  -- 🌟 ATMOSPHERE
  JSON_VALUE(Caracteristiques, '$.atmosphere.casual') = 'casual' AS is_casual,
  JSON_VALUE(Caracteristiques, '$.atmosphere.cozy') = 'cozy' AS is_cozy,
  JSON_VALUE(Caracteristiques, '$.atmosphere.quiet') = 'quiet' AS is_quiet,
  JSON_VALUE(Caracteristiques, '$.atmosphere.romantic') = 'romantic' AS is_romantic,
  JSON_VALUE(Caracteristiques, '$.atmosphere.trendy') = 'trendy' AS is_trendy,
  JSON_VALUE(Caracteristiques, '$.atmosphere.upscale') = 'upscale' AS is_upscale,

  -- 👥 CROWD
  JSON_VALUE(Caracteristiques, '$.crowd.family-friendly') = 'family-friendly' AS is_family_friendly,
  JSON_VALUE(Caracteristiques, '$.crowd.tourists') = 'popular-with-tourists' AS popular_with_tourists,

  -- 📅 PLANNING
  JSON_VALUE(Caracteristiques, '$.planning.accepts-reservations') = 'accepts-reservations' AS accepts_reservations
  
FROM {{ ref('restau_cate_valide_2') }}
--where nom like '%bouillon%'