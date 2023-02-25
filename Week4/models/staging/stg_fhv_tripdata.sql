{{ config(materialized="view") }}


with tripdata as 
(
    select *
    from {{source('staging', 'non_partitioned_fhv_tripdata')}}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019) 
)


select 
    --identifiers
    cast(int64_field_0 as integer) as tripid,
    cast(PUlocationID as integer) as pu_locationID,
    cast(DOlocationId as integer) as do_locationId,

    --timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    --tripinfo
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(SR_Flag as string) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from tripdata

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}



