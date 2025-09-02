with 

source as (

    select * from {{ source('RESTAUPARIS', 'INSEE_etablissement') }}

),

renamed as (

    select
    *,
        INITCAP(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                TRANSLATE(
                    LOWER(
                    CASE
                        WHEN enseigne1Etablissement IS NULL THEN COALESCE(denominationusuelle1unitelegale, denominationunitelegale)
                        WHEN COALESCE(denominationunitelegale, denominationusuelle1unitelegale) IS NULL THEN denominationusuelle1unitelegale
                        ELSE enseigne1Etablissement
                    END
                    ),
                    'àâäáãçèéêëìíîïñòóôöõùúûüýÿÀÂÄÁÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ',
                    'aaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY'
                ),
                r'\b(soc|sas|societe|sarl)\b', ''   -- supprime les mots juridiques
                ),
                r'[^a-zA-Z0-9 ]', ''                  -- supprime caractères spéciaux
            )
            ) AS nom,
        datecreationunitelegale
        as date_creation,
        case
        when etatadministratifunitelegale = 'A' then true
        when etatadministratifunitelegale = 'C' then false
        else null
        end as actif,
        INITCAP(lower(
            concat(
                numerovoieetablissement,
                ' ',
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                typevoieetablissement,
                                'AVENUE', 'av'
                            ),
                            'BOULEVARD', 'bd'
                        ),
                        'IMPASSE', 'imp'
                    ),
                    'PLACE', 'pl'
                ),
                ' ',
                libellevoieetablissement
            )
        )) as adresse_1,
        codepostaletablissement
        as code_postal,
        CASE 
            WHEN caractereemployeuretablissement ='O' THEN TRUE
            ELSE FALSE
        END as employeur
    from source
-- where etatadministratifunitelegale = 'A'
)

select
concat (nom,' - ',adresse_1) as PK,
* from renamed
-- where nom like '%Bouillon%'