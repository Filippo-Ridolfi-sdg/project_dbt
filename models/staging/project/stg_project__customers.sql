with 

source_t0 as (

    select * from {{ source('project', 'customers')}}
),

source_t1 as (

    select * from {{ source('project', 'customers_t1')}}
),


transformed as (

    select
        coalesce(source_t1.customer_id, source_t0.customer_id) as customer_id,
        coalesce(source_t1.name, source_t0.name) as customer_name,
        coalesce(source_t1.email, source_t0.email) as customer_email,
        coalesce(source_t1.city, source_t0.city) as customer_city,
        case
            when source_t0.customer_id is null then 'NEW'
            when source_t1.customer_id is null then 'DELETED'
            when (source_t1.name <> source_t0.name)
              or (source_t1.email <> source_t0.email)
              or (source_t1.city <> source_t0.city)
                then 'UPDATED'
            else 'UNCHANGED'
        end as state_customers
    from source_t0
    full outer join source_t1 using (customer_id)
)

select * from transformed
