with 

source as (

    select * from {{ source('RESTAUPARIS', 'INSEE_etablissement') }}

),

renamed as (

    select
        regexp_replace(
            lower(
            case 
                when enseigne1Etablissement is null then 
                    regexp_replace(lower(denominationusuelle1unitelegale), 'soc |sas |societe|sarl', '')
                when coalesce(denominationunitelegale, denominationusuelle1unitelegale) is null then 
                    regexp_replace(lower(denominationunitelegale), 'soc |sas |societe|sarl', '')
                else 
                    regexp_replace(lower(enseigne1Etablissement), 'soc |sas |societe|sarl', '')
            end),
            r'[^a-zA-Z0-9 ]', '')
        as nom,
        datecreationetablissement
        as date_creation,
        case
        when etatadministratifunitelegale = 'A' then true
        when etatadministratifunitelegale = 'C' then false
        else null
        end as actif,
        lower(
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
        ) as adresse_1,
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
