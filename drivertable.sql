#cleaning the ride table
CREATE TABLE cleaned_ride_data AS
SELECT 
    driver_id,
    ride_id,
    ride_prime_time / 100.0 AS "ride_prime_time(percentage)",
    ride_distance / 1609.344 AS "ride_distance(miles)",
    ride_duration / 60.0 AS "ride_duration(minutes)",
    2 + ((ride_distance / 1609.344) * 1.15) + ((ride_duration / 60.0) * 0.22) + 1.75 AS "cost w/o prime time",
    1.75 + (2 + ((ride_distance / 1609.344) * 1.15) + ((ride_duration / 60.0) * 0.22)) * (1 + (ride_prime_time / 100.0) + 0.085) AS "cost w/ prime time and tax",
    (CASE 
        WHEN (1.75 + (2 + ((ride_distance / 1609.344) * 1.15) + ((ride_duration / 60.0) * 0.22)) * (1 + (ride_prime_time / 100.0))) < 5 THEN 5
        WHEN (1.75 + (2 + ((ride_distance / 1609.344) * 1.15) + ((ride_duration / 60.0) * 0.22)) * (1 + (ride_prime_time / 100.0))) > 400 THEN 400
        ELSE (1.75 + (2 + ((ride_distance / 1609.344) * 1.15) + ((ride_duration / 60.0) * 0.22)) * (1 + (ride_prime_time / 100.0)))
    END - 1.75) * 0.2 AS lyft_profit
FROM 
    ride;



#creating a new table by joining the entries in the cleaned ride table and the driver table
CREATE TABLE merge AS
SELECT 
    c.driver_id,
    c.ride_id,
    c.`ride_prime_time(percentage)`,
    c.`ride_distance(miles)`,
    c.`ride_duration(minutes)`,
    c.`cost w/o prime time`,
    c.`cost w/ prime time and tax`,
    c.lyft_profit,
    d.driver_onboard_date  -- Include this column from the driver table
FROM 
    cleaned_ride_data c
INNER JOIN 
    driver d
ON 
    c.driver_id = d.driver_id;

#cleaning the time table
CREATE TABLE cleaned_time AS
WITH pivoted_data AS (
    SELECT 
        ride_id,
        MAX(CASE WHEN event = 'requested_at' THEN 
            CASE WHEN timestamp != '' THEN STR_TO_DATE(timestamp, '%Y-%m-%d %H:%i:%s') ELSE NULL END
        END) AS requested_at,
        MAX(CASE WHEN event = 'accepted_at' THEN 
            CASE WHEN timestamp != '' THEN STR_TO_DATE(timestamp, '%Y-%m-%d %H:%i:%s') ELSE NULL END
        END) AS accepted_at,
        MAX(CASE WHEN event = 'arrived_at' THEN 
            CASE WHEN timestamp != '' THEN STR_TO_DATE(timestamp, '%Y-%m-%d %H:%i:%s') ELSE NULL END
        END) AS arrived_at,
        MAX(CASE WHEN event = 'picked_up_at' THEN 
            CASE WHEN timestamp != '' THEN STR_TO_DATE(timestamp, '%Y-%m-%d %H:%i:%s') ELSE NULL END
        END) AS picked_up_at,
        MAX(CASE WHEN event = 'dropped_off_at' THEN 
            CASE WHEN timestamp != '' THEN STR_TO_DATE(timestamp, '%Y-%m-%d %H:%i:%s') ELSE NULL END
        END) AS dropped_off_at
    FROM time
    GROUP BY ride_id
)
SELECT 
    ride_id,
    requested_at,
    accepted_at,
    arrived_at,
    picked_up_at,
    dropped_off_at,
    TIMESTAMPDIFF(SECOND, requested_at, accepted_at) AS duration_request_to_accept,
    TIMESTAMPDIFF(SECOND, accepted_at, arrived_at) AS duration_accept_to_arrive,
    CASE 
        WHEN picked_up_at >= arrived_at THEN TIMESTAMPDIFF(SECOND, arrived_at, picked_up_at)
        ELSE NULL
    END AS duration_arrived_to_pickup,
    TIMESTAMPDIFF(SECOND, picked_up_at, dropped_off_at) AS duration_ride
FROM pivoted_data;

CREATE TABLE final_table AS
SELECT 
    m.driver_id,
    m.ride_id,
    m.`ride_prime_time(percentage)`,
    m.`ride_distance(miles)`,
    m.`ride_duration(minutes)`,
    m.`cost w/o prime time`,
    m.`cost w/ prime time and tax`,
    m.lyft_profit,
    m.driver_onboard_date,
    ct.requested_at,
    ct.accepted_at,
    ct.arrived_at,
    ct.picked_up_at,
    ct.dropped_off_at,
    ct.duration_request_to_accept,
    ct.duration_accept_to_arrive,
    ct.duration_arrived_to_pickup,
    ct.duration_ride
FROM 
    merge m
INNER JOIN 
    cleaned_time ct
ON 
    m.ride_id = ct.ride_id;

CREATE TABLE Final_Table AS
SELECT 
    d.driver_id,
    d.driver_onboard_date,
    crd.ride_id,
    crd.`ride_prime_time(percentage)`,
    crd.`ride_distance(miles)`,
    crd.`ride_duration(minutes)`,
    crd.`cost w/o prime time`,
    crd.`cost w/ prime time and tax`,
    crd.lyft_profit,
    ct.requested_at,
    ct.accepted_at,
    ct.arrived_at,
    ct.picked_up_at,
    ct.dropped_off_at,
    ct.duration_request_to_accept,
    ct.duration_accept_to_arrive,
    ct.duration_arrived_to_pickup,
    ct.duration_ride
FROM 
    driver d
INNER JOIN 
    cleaned_ride_data crd ON d.driver_id = crd.driver_id
INNER JOIN 
    cleaned_time ct ON crd.ride_id = ct.ride_id;
    