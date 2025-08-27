with 

source as (

    select * from {{ source('RESTAUPARIS', 'testbapt5') }}

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
        tags_des_avis,
        nombre_de_photos,
        caracteristiques,
        liens_de_resa,
        liens_de_commande,
        rn,
        type_principal_normalise

    from source

)

select * from renamed
