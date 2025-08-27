SELECT
    CONCAT(
        REGEXP_REPLACE(LOWER(nom), r'[^a-zA-Z0-9 ]', ''),
        ' - ',
        REGEXP_REPLACE(
            LOWER(
                REGEXP_REPLACE(
                    LOWER(
                        REGEXP_REPLACE(
                            LOWER(
                                REGEXP_REPLACE(
                                    LOWER(
                                        REGEXP_REPLACE(
                                            LOWER(adresse_1),
                                            r'r\. ', 'rue'
                                        )
                                    ),
                                    r'av\. ', 'av'
                                )
                            ),
                            r'bd\.|boulevard', 'bd'
                        )
                    ),
                    r'impasse|imp\. ', 'imp'
                )
            ),
            r'place |pl\. ', 'pl'
        )
    ) AS PK,
    *
FROM {{ ref('restau_merged_jeff_bapt') }}
