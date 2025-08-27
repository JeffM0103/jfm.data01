with 

source as (

    select * from {{ source('RESTAUPARIS', 'INSEE_etablissement') }}

),

renamed as (

    select
        datecreationetablissement,
        trancheeffectifsetablissement,
        activiteprincipaleregistremetiersetablissement,
        etablissementsiege,
        etatadministratifunitelegale,
        datecreationunitelegale,
        categoriejuridiqueunitelegale,
        denominationunitelegale,
        denominationusuelle1unitelegale,
        denominationusuelle2unitelegale,
        complementadresseetablissement,
        numerovoieetablissement,
        typevoieetablissement,
        libellevoieetablissement,
        codepostaletablissement,
        libellecommuneetablissement,
        identifiantadresseetablissement,
        coordonneelambertabscisseetablissement,
        coordonneelambertordonneeetablissement,
        etatadministratifetablissement,
        enseigne1etablissement,
        activiteprincipaleetablissement,
        nomenclatureactiviteprincipaleetablissement,
        caractereemployeuretablissement

    from source

)

select * from renamed
