with 

source as (

    select * from {{ source('RESTAUPARIS', 'RestauParis_clean_V2') }}

),

restau_paris as (

    select
        nom,
        description_1,
        description_2,
        description_3,
        est_ferm___d__finitivement,
        type_principal,
        tous_les_types,
        site_internet,
        site_internet__url_racine_,
        t__l__phone,
        t__l__phone_international,
        adresse_compl__te,
        district,
        adresse_1,
        adresse_2,
        ville,
        code_postal,
        __tat,
        division_de_niveau_1,
        division_de_niveau_2,
        pays,
        code_pays,
        longitude,
        latitude,
        lien,
        nom_du_propri__taire,
        id_du_propri__taire,
        place_id,
        email,
        lien_facebook,
        lien_instagram,
        vu_pour_la_premi__re_fois_le,
        vu_pour_la_premi__re_fois_le__timestamp_,
        gamme_de_prix,
        id_des_avis,
        nombre_d_avis,
        note_des_avis,
        note_des_avis_texte_brut,
        nombre_d_avis_par_note,
        tags_des_avis,
        nombre_de_photos,
        photo_1,
        photo_2,
        occupation,
        est_revendiqu__,
        heures_d_ouverture,
        caract__ristiques,
        liens_de_r__servation,
        liens_de_commande,
        lien_des_offres,
        titre_du_site

    from source

)

restau_clean1 as (
select
Nom,
CONCAT (Description_1," - ",Description_2," - ",Description_3) AS Description,
CASE 
  WHEN Est_ferm___d__finitivement="Oui" THEN FALSE
  ELSE TRUE
END AS Ouvert,
Type_principal,	
Tous_les_types,
CASE 
  WHEN Site_internet IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Site_internet,	
CASE 
  WHEN T__l__phone IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Telephone,
Adresse_compl__te AS Adresse_complete,
Adresse_1,
Code_postal,	
Longitude,
Latitude,
Lien,
CASE 
  WHEN Email IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Email,
CASE 
  WHEN Lien_Facebook IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Lien_Facebook,
CASE 
  WHEN Lien_Instagram IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Lien_Instagram,
CASE 
  WHEN Gamme_de_prix = "$" OR Gamme_de_prix = "€" OR Gamme_de_prix = "£"  THEN 1
  WHEN Gamme_de_prix = "$$" OR Gamme_de_prix = "€€" OR Gamme_de_prix = "££"  THEN 2
  WHEN Gamme_de_prix = "$$$" OR Gamme_de_prix = "€€€" OR Gamme_de_prix = "£££"  THEN 3
  WHEN Gamme_de_prix = "$$$$" OR Gamme_de_prix = "€€€€" OR Gamme_de_prix = "££££"  THEN 4
  ELSE null
END AS Gamme_de_prix,
CAST(Nombre_d_avis AS INT64) AS Nombre_d_avis,
CAST(Note_des_avis_texte_brut AS FLOAT64) AS Note_avis,
Tags_des_avis,
CAST(REPLACE(Nombre_de_photos, '+', '') AS INT64) AS Nombre_de_photos,	
Caract__ristiques AS Caracteristiques,
CASE 
  WHEN Liens_de_r__servation IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Liens_de_resa,
CASE 
  WHEN Liens_de_commande IS NOT NULL THEN TRUE
  ELSE FALSE
END AS Liens_de_commande
from restau_paris
)
,
-- On va récupérer les informations dans "Description", "type_principal" et "tous_les types" pour identifier les types de cuisine
base as (
    select *,
      array_to_string(
        array(
          select x from unnest([
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'asia') then 'Restaurant asiatique' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'couscous|tagine') then 'Restaurant marocain' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'français|corse|savoyard|sud ouest|alsac') then 'Restaurant français' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'fruit de mer|fruits de la mer|de la mer') then 'Restaurant de fruits de mer' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'mexicain|mexique') then 'Restaurant mexicain' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'itali') then 'Restaurant italien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'mauric|antill|martiniqu') then 'Restaurant créole' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'méditerr') then 'Restaurant méditerranéen' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'espagn') then 'Restaurant espagnol' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'sushi|ramen|jap') then 'Restaurant japonais' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'sri lanka|bengla|indien|indian') then 'Restaurant indien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'américain|hamburger') then 'Restaurant à hamburgers' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'turc|turk') then 'Restaurant turc' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'argentin') then 'Restaurant argentin' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'thai|thaï') then 'Restaurant thai' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'chine|chinois|hot pot|kong') then 'Restaurant chinois' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'tunis') then 'Restaurant tunisien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'coréen|korean') then 'Restaurant coréen' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'poke|bowl|hawa') then 'Restaurant hawaien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'kebab|döner') then 'Kebab' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'grec') then 'Restaurant grec' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'bagel') then 'Restaurant à bagel' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'colombi|péru|cuba') then 'Restaurant latino américain' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'brésil|brazil') then 'Restaurant brésilien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'éthiopi') then 'Restaurant éthiopien' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'liban') then 'Restaurant libanais' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'crêperie') then 'Crêperie' end,
            case when regexp_contains(lower(coalesce(description,'') || coalesce(type_principal,'') || coalesce(tous_les_types,'')), r'viet|viêt') then 'Restaurant vietnamien' end
          ]) x
          where x is not null
        ), ', '
      ) as new_cat_from_description
    from source
)

select * from base
