with source as (

    select * from {{ ref('stg_acea_data') }}

),

renamed as (

    select
        region as region,
        manufacturer as manufacturer,
        frequency as frequency,
        month as month,
        units as units,
        pdf_name as pdf_name,
        inserted_at as inserted_at

    from source

),

{# need to dedupe because there may be multiple entries across pdfs #}
deduping as (
    select *
        , row_number() over (
            partition by region, manufacturer, frequency, month
            order by pdf_name desc
        ) as row_deduped
    from source
),

deduped as (
    select * exclude(row_deduped)
    from deduping
    where row_deduped = 1
),    

surrogate_key as (
    select *
        , {{ dbt_utils.generate_surrogate_key(['manufacturer', 'month', 'frequency', 'region']) }} as id
    from deduped
)

select * from surrogate_key