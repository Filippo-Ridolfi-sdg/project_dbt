with 

source_t0 as ( 
    select * from {{ source('project', 'customers_t0') }}
),

source_t1 as ( 
    select * from {{ source('project', 'customers_t1') }}
),

select_t0 as (

    select 
        {{ dbt_utils.generate_surrogate_key([
            'customer_cd'
        ]) }} as customer_id,

        customer_cd as customer_cd,
        name as customer_name,
        email as customer_email,
        city as customer_city,
        member_since,
        cast(last_update as timestamp) as last_update,
        
        coalesce(is_deleted, 0) = 1 as is_deleted

    from source_t0
),

select_t1 as (

    select 
        {{ dbt_utils.generate_surrogate_key([
            'customer_cd'
        ]) }} as customer_id,

        customer_cd as customer_cd,
        name as customer_name,
        email as customer_email,
        city as customer_city,
        member_since,
        
        cast(last_update as timestamp) as last_update,
        
        coalesce(is_deleted, 0) = 1 as is_deleted

    from source_t1
),

-- Passaggio 1: Unisci tutti i record da entrambe le fonti
unioned_data as (

    select * from select_t0
    
    union all
    
    select * from select_t1
),

-- Passaggio 2: Assegna un numero di riga (rango) a ogni record per cliente,
-- ordinando dal più recente (rn = 1) al più vecchio
ranked_data as (

    select
        *,
        row_number() over (
            partition by customer_id     -- Per ogni cliente
            order by last_update desc    -- Ordina per data (la più recente prima)
        ) as rn
    
    from unioned_data
),

-- Passaggio 3: Filtra per tenere solo il record più recente (rn = 1) per ogni cliente
final as (

    select
        customer_id,
        customer_cd,
        customer_name,
        customer_email, 
        customer_city, 
        member_since,
        last_update,
        is_deleted
        
    from ranked_data
    
    where rn = 1  -- Questo seleziona solo la riga più aggiornata
)

-- Passaggio 4: Seleziona i dati finali, escludendo quelli marcati come eliminati
select * from final
where is_deleted = False

-- final as (

--     select
--         t1.customer_id,
--         t1.customer_cd,
--         t1.customer_name,
--         t1.customer_email, 
--         t1.customer_city, 
--         t1.member_since,
--         t1.last_update,
--         t1.is_deleted
        
--     from select_t1 as t1
    
--     left join select_t0 as t0
--         on t1.customer_id = t0.customer_id
    
--     where 
--         t0.customer_id is null 
        
--         or (t1.last_update > t0.last_update)
        
--         or (t1.last_update = t0.last_update)
        
-- )


-- select * from final
-- where is_deleted = False