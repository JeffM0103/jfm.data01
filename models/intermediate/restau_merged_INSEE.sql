select
    r.*,
    case
        when
            regexp_contains(lower(r.nom), r'gastronomique')
        or regexp_contains(lower(r.description), r'gastronomique')
        or regexp_contains(lower(r.type_principal), r'gastronomique')
        or regexp_contains(lower(r.tous_les_types), r'gastronomique')
        then true
        else false
    end as gastronomique,
    case
    when
        regexp_contains(lower(r.nom), r'restauration rapide')
        or regexp_contains(lower(r.description), r'restauration rapide')
        or regexp_contains(lower(r.type_principal), r'restauration rapide')
        or regexp_contains(lower(r.tous_les_types), r'restauration rapide')
    then true
    else false
end as restauration_rapide,

    case
        when
            regexp_contains(lower(r.nom), r' fusion')
        or regexp_contains(lower(r.description), r' fusion')
        or regexp_contains(lower(r.type_principal), r' fusion')
        or regexp_contains(lower(r.tous_les_types), r' fusion')
        then true
        else false
    end as cuisine_fusion,
    case
        when
           regexp_contains(lower(r.nom), r'brasserie|bistro')
        or regexp_contains(lower(r.description), r'brasserie|bistro')
        or regexp_contains(lower(r.type_principal), r'brasserie|bistro')
        or regexp_contains(lower(r.tous_les_types), r'brasserie|bistro')
        then true
        else false
    end as brasserie,

    case
        when
        regexp_contains(lower(r.nom), r'grillade')
        or regexp_contains(lower(r.description), r'grillade')
        or regexp_contains(lower(r.type_principal), r'grillade')
        or regexp_contains(lower(r.tous_les_types), r'grillade')
        then true
        else false
    end as grillade,
    case
        when
            regexp_contains(lower(r.nom), r'tapas')
        or regexp_contains(lower(r.description), r'tapas')
        or regexp_contains(lower(r.type_principal), r'tapas')
        or regexp_contains(lower(r.tous_les_types), r'tapas')
        then true
        else false
    end as tapas,
    case
        when
            regexp_contains(lower(r.nom), r'[aà] volonté')
        or regexp_contains(lower(r.description), r'[aà] volonté')
        or regexp_contains(lower(r.type_principal), r'[aà] volonté')
        or regexp_contains(lower(r.tous_les_types), r'[aà] volonté')
        then true
        else false
    end as a_volonte,
    case
        when
           regexp_contains(lower(r.type_principal), r'bar')
        or regexp_contains(lower(r.tous_les_types), r'bar')
        then true
        else false
    end as bar,
    i.date_creation,
    i.actif,
    i.employeur
from {{ ref("prep_PK") }} as r
left join {{ ref("stg_RESTAUPARIS__INSEE_etablissement") }} as i using (PK)
