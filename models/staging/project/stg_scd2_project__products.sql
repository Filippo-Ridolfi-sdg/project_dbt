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
-- 1. Identifica i record NUOVI o MODIFICATI confrontando tutti i campi rilevanti
changed_records_source as (
    select
        s.*
    from source_data as s
    where not exists (
        select 1
        from {{ this }} as t
        where 
            -- a. Troviamo la corrispondenza per la chiave naturale
            t.product_cd = s.product_cd
            -- b. Troviamo la versione ATTIVA
            and t.is_current = true
            and COALESCE(t.category, 'DBT_NULL_STR') = COALESCE(s.category, 'DBT_NULL_STR')
            and COALESCE(t.list_price, 0) = COALESCE(s.list_price, 0)
            and COALESCE(t.color, 'DBT_NULL_STR') = COALESCE(s.color, 'DBT_NULL_STR')
            and COALESCE(t.is_deleted, false) = COALESCE(s.is_deleted, false)
    )
),
{% endif %}

-- 2. Righe da INSERIRE (Nuovi record o nuove versioni di record esistenti)
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
        true as is_current         
    from 
    {% if is_incremental() %}
        changed_records_source 
    {% else %}
        source_data                
    {% endif %}
),

{% if is_incremental() %}
-- 3. Righe da AGGIORNARE/CHIUDERE (Le vecchie versioni che devono essere contrassegnate come non più correnti)
rows_to_update as (
    select
        t.product_cd,
        t.model_name,
        t.brand,
        t.category,
        t.list_price,
        t.color,
        t.is_deleted,
        t.dbt_updated_at,
        false as is_current
    from {{ this }} as t
    -- Unisci con i record sorgente modificati per trovare i record 'is_current = true' da chiudere
    inner join changed_records_source s
        on t.product_cd = s.product_cd
    where t.is_current = true
),
{% endif %}

-- Finalizzazione
final as (
    select * from rows_to_insert
    
    {% if is_incremental() %}
    union all
    select * from rows_to_update
    {% endif %}
)

select * from final