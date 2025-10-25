with 

source as (

    select * from {{ source('project', 'customers')}}
),

transformed as (

    select 
        customer_id,
        name as customer_name,
        email as customer_email,
        city as customer_city,
        member_since,
        current_timestamp() as loaded_at
        
    from source
)

select * from transformed