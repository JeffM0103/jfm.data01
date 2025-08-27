SELECT
    *,
    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r'gastronomique')
            THEN TRUE ELSE FALSE
    END AS gastronomique,

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r'restauration rapide')
            THEN TRUE ELSE FALSE
    END AS restauration_rapide, 

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r' fusion')
            THEN TRUE ELSE FALSE
    END AS cuisine_fusion,

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r'brasserie|bistro ')
            THEN TRUE ELSE FALSE
    END AS brasserie,

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r'grillade')
            THEN TRUE ELSE FALSE
    END AS grillade,

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r' tapas')
            THEN TRUE ELSE FALSE
    END AS tapas,

    CASE
        WHEN regexp_contains(coalesce(lower(nom), lower(description), lower(type_principal), lower(tous_les_types)), r'[aà] volonté')
            THEN TRUE ELSE FALSE
    END AS a_volonte,

    CASE
        WHEN regexp_contains(coalesce(lower(type_principal), lower(tous_les_types)), r'bar')
            THEN TRUE ELSE FALSE
    END AS bar
FROM {{ ref('restau_merged_jeff_bapt') }}
