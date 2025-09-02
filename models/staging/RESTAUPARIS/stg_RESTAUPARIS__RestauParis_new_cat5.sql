with 

source as (

    select * from {{ source('RESTAUPARIS', 'RestauParis_new_cat5') }}

),

renamed as (

    select
            INITCAP(
            REGEXP_REPLACE(
                TRANSLATE(
                    Nom,
                    'àâäáãåçèéêëìíîïñòóôöõùúûüýÿÀÂÄÁÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ',
                    'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY'
                ),
                r'[^a-zA-Z0-9 ]', ' '                     -- supprime caractères spéciaux
            )
        ) as nom,
        lower(description) as description,
        ouvert,
        type_principal,
        tous_les_types,
        case 
            when regexp_contains(lower(description), r'grillade|hot dog|californien')
             and lower(new_cat_cleaned) like '%hamburger%'
            then null
            when regexp_contains(new_cat_cleaned, r'fruits de mer')
            then 'Restaurant de fruits de mer'
            when regexp_contains(new_cat_cleaned, r'kebab')
            then 'Kebab'
            else new_cat_cleaned
        end as new_cat_cleaned,
        lower(adresse_complete) as adresse_complete,
        lower(adresse_1) as adresse_1,
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
        lien as Lien_maps,
    from source

)

select * from renamed
where Nom like '%Bouillon%'