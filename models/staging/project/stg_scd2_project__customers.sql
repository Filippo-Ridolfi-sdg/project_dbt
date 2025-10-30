{{ 
    config(
        materialized = 'incremental',
        unique_key = 'customer_update_id',
        incremental_strategy = 'merge'
    ) 
}}

/*
    Carica i dati di origine dalla tabella appropriata in base al tipo di esecuzione
*/

with source_data as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['customer_cd', 'last_update']) }} as customer_update_id
    from 
    {% if is_incremental() %}
    {{ source('project', 'customers_t1') }}
    {% else %}
    {{ source('project', 'customers_t0') }}
    {% endif %}
),

{% if is_incremental() %}

/*
    Isola solo i record della sorgente che sono nuovi o modificati
*/
changed_records_source as (
    select
        s.*
    from source_data s
    -- Solo i record la cui 'customer_update_id' NON esiste già nel modello ({{ this }})
    left join {{ this }} as t
        on s.customer_update_id = t.customer_update_id
    where t.customer_update_id is null
),
{% endif %}

/*
    Prepara le nuove versioni dei record
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
    {% if is_incremental() %}
        changed_records_source     
    {% else %}
        source_data                
    {% endif %}
),

{% if is_incremental() %}

/*
    Prepara i record esistenti da aggiornare/chiudere
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
        s.last_update as valid_to, 
        false as is_current        
    from {{ this }} as t
    
    inner join changed_records_source s
        on t.customer_cd = s.customer_cd
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