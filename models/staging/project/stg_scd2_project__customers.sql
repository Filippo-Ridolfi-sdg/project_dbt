{{ 
    config(
        materialized = 'incremental',
        unique_key = 'customer_update_id',
        incremental_strategy = 'merge'
    ) 
}}

with source_data as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['customer_cd', 'last_update']) }} as customer_update_id
    from 
    {% if is_incremental() %}
        {{ source('project', 'customers_t1') }}

        where last_update > (select max(last_update) from {{ this }})
    {% else %}
        {{ source('project', 'customers_t0') }}
    {% endif %}
),

/*
    Prepara le nuove versioni dei record
    'source_data' contiene GIÀ solo i record nuovi o modificati
*/
rows_to_insert as (
    select
        customer_update_id,
        customer_cd,
        name,
        email,
        city,
        member_since,
        last_update,
        is_deleted,
        current_timestamp() as dbt_updated_at,
        last_update as valid_from,
        null as valid_to,          
        true as is_current         
    from 
        source_data
),

{% if is_incremental() %}

/*
    Prepara i record esistenti da aggiornare/chiudere
    Questi sono i record GIÀ presenti in {{ this }} che devono essere chiusi
*/
rows_to_update as (
    select
        t.customer_update_id,
        t.customer_cd,
        t.name,
        t.email,
        t.city,
        t.member_since,
        t.last_update,
        t.is_deleted,
        t.dbt_updated_at,
        t.valid_from,
        s.last_update as valid_to, -- Prende la data di aggiornamento dal nuovo record
        false as is_current        
    from {{ this }} as t
    
    -- Unisce i record sorgente (nuovi) con la tabella di destinazione (attuale)
    -- sulla chiave di business (customer_cd)
    inner join source_data s
        on t.customer_cd = s.customer_cd
    -- Filtra solo i record attualmente attivi in destinazione
    where t.is_current = true
),

{% endif %}

/*
    Combina i record da inserire e i record esistenti da aggiornare/chiudere
*/
final as (
    select * from rows_to_insert
    
    {% if is_incremental() %}
    union all
    select * from rows_to_update
    {% endif %}
)

select * from final