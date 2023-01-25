-- Q3: 20530
select
	count(*) num_rides
from ny_taxi.public.fct_green_trips
where 
	lpep_pickup_datetime::date = '2019-01-15'
	and lpep_dropoff_datetime::date = '2019-01-15'
	
-- Q4: 2019-01-15
select
	lpep_pickup_datetime::date,
	max(trip_distance) max_trip_distance
from ny_taxi.public.fct_green_trips
group by 1
order by max_trip_distance desc


-- Q5: 2: 1282 ; 3: 254
select
	passenger_count,
	count(*) num_rides
from ny_taxi.public.fct_green_trips
where lpep_pickup_datetime::date = '2019-01-01'
group by 1
order by 1 asc


-- Q6: Long Island City/Queens Plaza
SELECT dz."Zone"
FROM
(SELECT gt."DOLocationID", max(tip_amount) max_tip
FROM ny_taxi.public.fct_green_trips gt
LEFT JOIN ny_taxi.public.dim_zones dz
ON gt."PULocationID" = dz."LocationID"
WHERE dz."Zone" = 'Astoria'
GROUP BY 1
ORDER BY max_tip DESC
) mt
LEFT JOIN ny_taxi.public.dim_zones dz
ON mt."DOLocationID" = dz."LocationID"
ORDER BY mt.max_tip DESC
LIMIT 1
