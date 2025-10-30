{{ 
    config(
        materialized = 'incremental',
        unique_key = 'product_cd',
        incremental_strategy = 'merge'
    ) 
}}
with 

source as (
    select
        *
    {% if is_incremental() %}
    from {{ source('project', 'products_t1') }}
    {% else %}
    from {{ source('project', 'products_t0') }}
    {% endif %}
),

/*
    CALCOLO DEI RECORD NUOVI O MODIFICATI
    Si vuole isolare solo i record che sono veramente cambiati (operazione MINUS)
    Risultato contiene solo i prodotti nuovi o i prodotti esistenti in cui almeno un campo è cambiato
*/

{% if is_incremental() %}
new_records as (
    select * from source
    MINUS
    select product_cd,
        model_name,
        brand,
        category,
        list_price,
        color,
        is_deleted from {{ this }}
),
{% endif %}

/*
    CALCOLO DEL DELTA
    CTE se in modalità incrementale, new_records
    CTE se non in modalità incrementale, source
*/

delta_calc as (
    select
        product_cd,
        model_name,
        brand,
        category,
        list_price,
        color,
        is_deleted

    from
    {% if is_incremental() %}
        new_records
    {% else %}
        source
    {% endif %}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_cd'])  }} as product_id,
        *,
        current_timestamp() as dbt_updated_at
    from delta_calc
)

select * from final