with 

source as (

    select * from {{ source('RESTAUPARIS', 'cat_restau_abdellah') }}

),

renamed as (

    select
        nom,
        description,
        ouvert,
        type_principal,
        tous_les_types,
        site_internet,
        telephone,
        adresse_complete,
        adresse_1,
        code_postal,
        longitude,
        latitude,
        lien,
        email,
        lien_facebook,
        lien_instagram,
        gamme_de_prix,
        nombre_d_avis,
        note_avis,
        nombre_d_avis_par_note,
        tags_des_avis,
        nombre_de_photos,
        caracteristiques,
        liens_de_resa,
        liens_de_commande,
        action,
        d__tail_restaurant_2

    from source

)

select * from renamed
