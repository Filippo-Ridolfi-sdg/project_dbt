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

        customer_cd,
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

        customer_cd,
        name as customer_name,
        email as customer_email,
        city as customer_city,
        member_since,
        
        cast(last_update as timestamp) as last_update,
        
        coalesce(is_deleted, 0) = 1 as is_deleted

    from source_t1
),

unioned_data as (

    select * from select_t0
    
    union all
    
    select * from select_t1
),

distinct_versions as (

    select distinct *
    from unioned_data
),

final as (

    select
        customer_id,
        customer_cd,
        customer_name,
        customer_email, 
        customer_city, 
        member_since,
        is_deleted,
        
        last_update as valid_from,
        
        lead(last_update) over (
            partition by customer_id
            order by last_update asc
        ) as valid_to
        
    from distinct_versions
)

select * from final
where is_deleted = False