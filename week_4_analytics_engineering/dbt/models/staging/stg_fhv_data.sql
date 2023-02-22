{{config(materialized='view')}}

select 
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(Affiliated_base_number as string) as affileated_base_number
from {{ source('staging_taxi_data', 'fhv_data') }}

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}