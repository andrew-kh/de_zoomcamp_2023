select distinct affiliated_base_number from de-zoomcamp-2023-376110.trips_fhv.fhv_opt
where date(pickup_datetime) between date('2019-03-01') and date('2019-03-31')
