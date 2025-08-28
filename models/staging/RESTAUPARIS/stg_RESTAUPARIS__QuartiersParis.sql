with 

source as (

    select * from {{ source('RESTAUPARIS', 'QuartiersParis') }}

),

renamed as (

    select
        n_sq_qu,
        c_qu,
        c_quinsee,
        l_qu,
        c_ar,
        n_sq_ar,
        perimetre,
        surface,
        geometry_x_y,
        geometry,
        st_area_shape_,
        st_perimeter_shape_

    from source

)

select * from renamed
