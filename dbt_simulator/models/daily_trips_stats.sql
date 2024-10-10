select
    date_trunc('day', t.pickup_datetime) as pickup_date,
    z.borough as pickup_borough,
    count(*) as trips_started,
    sum(passenger_count) as total_passengers,
    avg(trip_distance) as average_trip_distance,
    sum(fare_amount) as total_fare_amount,
    sum(total_amount) as total_amount_charged
from {{ ref('taxi_trips') }} as t
left join {{ ref('taxi_zones') }} as z
    on z.location_id = t.pickup_location_id
group by all
