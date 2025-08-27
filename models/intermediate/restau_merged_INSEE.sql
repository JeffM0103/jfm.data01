select
    r.*,
    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r'gastronomique'
            )
        then true
        else false
    end as gastronomique,
    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r'restauration rapide'
            )
        then true
        else false
    end as restauration_rapide,

    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r' fusion'
            )
        then true
        else false
    end as cuisine_fusion,
    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r'brasserie|bistro '
            )
        then true
        else false
    end as brasserie,

    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r'grillade'
            )
        then true
        else false
    end as grillade,
    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r' tapas'
            )
        then true
        else false
    end as tapas,
    case
        when
            regexp_contains(
                coalesce(
                    lower(r.nom),
                    lower(r.description),
                    lower(r.type_principal),
                    lower(r.tous_les_types)
                ),
                r'[aà] volonté'
            )
        then true
        else false
    end as a_volonte,
    case
        when
            regexp_contains(
                coalesce(lower(r.type_principal), lower(r.tous_les_types)), r'bar'
            )
        then true
        else false
    end as bar,
    i.date_creation,
    i.actif,
    i.employeur
from {{ ref("prep_PK") }} as r
left join {{ ref("stg_RESTAUPARIS__INSEE_etablissement") }} as i using (PK)
where actif is not null
