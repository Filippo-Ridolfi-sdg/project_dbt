{{
    config(
        materialized = 'incremental',
        unique_key = ['product_cd','is_current'], 
        incremental_strategy = 'merge'
    ) 
}}

with source_data as (
    select
        *
    from 
    {% if is_incremental() %}
        {{ source('project', 'products_t1') }}
    {% else %}
        {{ source('project', 'products_t0') }}
    {% endif %}
),

{% if is_incremental() %}

/*
    Isola solo i record della sorgente che sono nuovi o modificati
*/
changed_records_source as (
    select
        s.*
    from source_data as s
    where not exists (
        select 1
        from {{ this }} as t
        where 
            t.product_cd = s.product_cd
            and t.is_current = true
            and t.category = s.category
            and t.list_price = s.list_price
            and t.color = s.color
            and coalesce(t.is_deleted, false) = coalesce(s.is_deleted, false)
    )
),
{% endif %}

/*
    Prepara le nuove versioni dei record
*/
rows_to_insert as (
    select
        product_cd,
        model_name,
        brand,
        category,
        list_price,
        color,
        is_deleted,
        current_timestamp() as dbt_updated_at,
        true as is_current,
        current_timestamp() as valid_from,
        cast(null as timestamp) as valid_to
    from 
    {% if is_incremental() %}
        changed_records_source 
    {% else %}
        source_data                
    {% endif %}
),

{% if is_incremental() %}

/*
    Preparazione delle righe da "chiudere" (solo in modalità incrementale)
    Imposta valid_to per i record non più attivi
*/
rows_to_update as (
    select
        t.product_id,
        t.product_cd,
        t.model_name,
        t.brand,
        t.category,
        t.list_price,
        t.color,
        t.is_deleted,
        t.dbt_updated_at,
        false as is_current,
        t.valid_from,
        current_timestamp() as valid_to
    from {{ this }} as t
    inner join changed_records_source s
        on t.product_cd = s.product_cd
    where t.is_current = true
),
{% endif %}

final as (
    select {{ dbt_utils.generate_surrogate_key(['product_cd', 'is_current']) }} as product_id,
        * 
    from rows_to_insert
    
    {% if is_incremental() %}
    union all
    select * from rows_to_update
    {% endif %}
)

select * from final
