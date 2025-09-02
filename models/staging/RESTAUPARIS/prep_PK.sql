SELECT
    INITCAP(
            REGEXP_REPLACE(
                TRANSLATE(
                    nom,
                    'àâäáãåçèéêëìíîïñòóôöõùúûüýÿÀÂÄÁÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ',
                    'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY'
                ),
                r'[^a-zA-Z0-9 ]', ' '                     -- supprime caractères spéciaux
            )
        ) as nom,
    CONCAT(
        INITCAP(
            REGEXP_REPLACE(
                TRANSLATE(
                    LOWER(nom),
                    'àâäáãåçèéêëìíîïñòóôöõùúûüýÿÀÂÄÁÃÅÇÈÉÊËÌÍÎÏÑÒÓÔÖÕÙÚÛÜÝ',
                    'aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY'
                ),
                r'[^a-zA-Z0-9 ]', ' '                     -- supprime caractères spéciaux
            )
        ),
        ' - ',
        INITCAP(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(adresse_1),
                            r'r\. ', 'rue'
                        ),
                        r'av\. ', 'av'
                    ),
                    r'bd\.|boulevard', 'bd'
                ),
                r'place |pl\. ', 'pl'
            )
        )
    ) AS PK,
* EXCEPT (nom)
FROM {{ ref('restau_merged_jeff_bapt') }}
-- where nom like '%bouillon%'