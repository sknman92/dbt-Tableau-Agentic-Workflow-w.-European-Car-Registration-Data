-- created_at: 2026-02-23T18:36:58.805085100+00:00
-- finished_at: 2026-02-23T18:36:59.218525900+00:00
-- elapsed: 413ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (with source as (

    select * from TIL_DATA_ENGINEERING.dbt_cyi_acea.stg_acea_data

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


deduped as (
    select *
        , first_value(units) over (
            partition by region, manufacturer, frequency, month
            order by pdf_name desc
        ) as units_deduped
    from renamed
    where manufacturer = 'BYD'
    order by region, manufacturer, frequency, month, pdf_name desc
    
),

surrogate_key as (
    select *
        , md5(cast(coalesce(cast(manufacturer as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(month as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(frequency as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(region as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
    from deduped
)

select * from surrogate_key
) limit 1000;
