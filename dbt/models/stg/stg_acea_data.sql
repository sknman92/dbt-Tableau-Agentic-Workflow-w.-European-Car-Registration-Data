with source as (

    select * from {{ source('dbt_cyi_acea', 'acea_data') }}

),

renamed as (

    select
        region as region,
        manufacturer as manufacturer,
        frequency as frequency,
        month as month,
        units as units,
        pdf as pdf_name,
        inserted_at as inserted_at

    from source

)

select * from renamed